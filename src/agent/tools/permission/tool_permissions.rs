use serde_json::Value;

use crate::permission::{PermissionContext, PermissionError, PermissionType};
use crate::tools::http_request::extract_domain_from_url;

const DELETE_COMMANDS: [&str; 7] = ["rm", "rmdir", "del", "erase", "unlink", "rd", "remove-item"];

pub fn check_permissions(
    tool_name: &str,
    args: &Value,
) -> Result<Option<Vec<PermissionContext>>, PermissionError> {
    match tool_name {
        "write_file" => {
            let path = required_string_arg(args, "path")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::WriteFile,
                path,
                format!("Write file: {}", path),
            )]))
        }
        "apply_patch" => {
            let path = required_string_arg(args, "path")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::WriteFile,
                path,
                format!("Apply patch to: {}", path),
            )]))
        }
        "execute_command" => {
            let command = required_string_arg(args, "command")?;
            let args_vec = args
                .get("args")
                .and_then(|value| value.as_array())
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_str().map(|s| s.to_string()))
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();
            let resource = build_command_resource(command, &args_vec);

            let mut contexts = Vec::new();
            if is_delete_command(command, &args_vec) {
                contexts.push(PermissionContext::new(
                    PermissionType::DeleteOperation,
                    resource.clone(),
                    format!("Delete operation via command: {}", resource),
                ));
            }

            contexts.push(PermissionContext::new(
                PermissionType::ExecuteCommand,
                resource.clone(),
                format!("Execute command: {}", resource),
            ));

            Ok(Some(contexts))
        }
        "git_write" => {
            let operation = required_string_arg(args, "operation")?;
            Ok(Some(vec![PermissionContext::new(
                PermissionType::GitWrite,
                operation,
                format!("Git write operation: {}", operation),
            )]))
        }
        "http_request" => {
            let url = required_string_arg(args, "url")?;
            let resource = extract_domain_from_url(url).unwrap_or_else(|| url.to_string());
            Ok(Some(vec![PermissionContext::new(
                PermissionType::HttpRequest,
                resource,
                format!("HTTP request to: {}", url),
            )]))
        }
        "terminal_session" => {
            let action = required_string_arg(args, "action")?;
            let resource = if action == "start" {
                required_string_arg(args, "command")?
            } else {
                required_string_arg(args, "session_id")?
            };

            Ok(Some(vec![PermissionContext::new(
                PermissionType::TerminalSession,
                resource,
                format!("Terminal session action: {}", action),
            )]))
        }
        _ => Ok(None),
    }
}

pub fn is_delete_command(command: &str, args: &[String]) -> bool {
    let command_lower = command.to_ascii_lowercase();
    if DELETE_COMMANDS
        .iter()
        .any(|delete| command_lower == *delete)
    {
        return true;
    }

    if args.iter().any(|arg| {
        let arg_lower = arg.to_ascii_lowercase();
        DELETE_COMMANDS.iter().any(|delete| arg_lower == *delete)
    }) {
        return true;
    }

    let mut raw_command = command_lower;
    if !args.is_empty() {
        raw_command.push(' ');
        raw_command.push_str(&args.join(" ").to_ascii_lowercase());
    }

    DELETE_COMMANDS
        .iter()
        .any(|delete| raw_command.contains(delete))
}

fn required_string_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, PermissionError> {
    args.get(key)
        .and_then(|value| value.as_str())
        .ok_or_else(|| PermissionError::CheckFailed(format!("Missing or invalid '{}' parameter", key)))
}

fn build_command_resource(command: &str, args: &[String]) -> String {
    let command = command.trim();
    if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn check_permissions_write_file() {
        let args = json!({"path": "/tmp/test.txt"});
        let contexts = check_permissions("write_file", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::WriteFile,
            "/tmp/test.txt",
            "Write file: /tmp/test.txt",
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_apply_patch() {
        let args = json!({"path": "/tmp/file.rs"});
        let contexts = check_permissions("apply_patch", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::WriteFile,
            "/tmp/file.rs",
            "Apply patch to: /tmp/file.rs",
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_execute_command_non_delete() {
        let args = json!({"command": "ls", "args": ["-la"]});
        let contexts = check_permissions("execute_command", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::ExecuteCommand,
            "ls -la",
            "Execute command: ls -la",
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_execute_command_delete() {
        let args = json!({"command": "rm", "args": ["-rf", "/tmp"]});
        let contexts = check_permissions("execute_command", &args).unwrap().unwrap();
        let delete_expected = PermissionContext::new(
            PermissionType::DeleteOperation,
            "rm -rf /tmp",
            "Delete operation via command: rm -rf /tmp",
        );
        let execute_expected = PermissionContext::new(
            PermissionType::ExecuteCommand,
            "rm -rf /tmp",
            "Execute command: rm -rf /tmp",
        );
        assert_eq!(contexts.len(), 2);
        assert!(contexts.contains(&delete_expected));
        assert!(contexts.contains(&execute_expected));
    }

    #[test]
    fn check_permissions_git_write() {
        let args = json!({"operation": "commit"});
        let contexts = check_permissions("git_write", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::GitWrite,
            "commit",
            "Git write operation: commit",
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_http_request_domain() {
        let url = "https://api.example.com/users";
        let args = json!({"url": url});
        let contexts = check_permissions("http_request", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::HttpRequest,
            "api.example.com",
            format!("HTTP request to: {url}"),
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_http_request_fallback() {
        let url = "ftp://example.com/file";
        let args = json!({"url": url});
        let contexts = check_permissions("http_request", &args).unwrap().unwrap();
        // extract_domain_from_url correctly extracts the domain from the URL
        let expected = PermissionContext::new(
            PermissionType::HttpRequest,
            "example.com",
            format!("HTTP request to: {url}"),
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_terminal_session_start() {
        let args = json!({"action": "start", "command": "npm run dev"});
        let contexts = check_permissions("terminal_session", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::TerminalSession,
            "npm run dev",
            "Terminal session action: start",
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn check_permissions_terminal_session_other_action() {
        let args = json!({"action": "kill", "session_id": "session-123"});
        let contexts = check_permissions("terminal_session", &args).unwrap().unwrap();
        let expected = PermissionContext::new(
            PermissionType::TerminalSession,
            "session-123",
            "Terminal session action: kill",
        );
        assert_eq!(contexts, vec![expected]);
    }

    #[test]
    fn is_delete_command_matches_command_arg_or_raw() {
        assert!(is_delete_command("rm", &[]));
        assert!(is_delete_command("echo", &["rm".to_string()]));
        assert!(is_delete_command(
            "bash",
            &["-lc".to_string(), "rm -rf /".to_string()]
        ));
        assert!(!is_delete_command("echo", &["hello".to_string()]));
    }
}
