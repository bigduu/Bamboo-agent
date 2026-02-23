use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

/// Tool for executing system commands
pub struct ExecuteCommandTool;

#[cfg(target_os = "windows")]
const SHELL: (&str, &str) = ("cmd", "/c");
#[cfg(not(target_os = "windows"))]
const SHELL: (&str, &str) = ("sh", "-c");

/// Command execution result
#[derive(Debug)]
pub struct CommandResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub success: bool,
}

impl ExecuteCommandTool {
    pub fn new() -> Self {
        Self
    }

    /// Execute a shell command (cross-platform)
    /// On Unix: uses `sh -c "command"`
    /// On Windows: uses `cmd /c "command"`
    pub async fn execute_shell(
        command_str: &str,
        cwd: Option<&str>,
        timeout_secs: u64,
    ) -> Result<CommandResult, String> {
        // Security check: block dangerous commands
        let dangerous_commands = ["rm -rf /", "rm -rf /*", "> /dev/sda", "dd if=/dev/zero"];
        for dangerous in &dangerous_commands {
            if command_str.contains(dangerous) {
                return Err(format!("Dangerous command blocked: {}", dangerous));
            }
        }

        let (shell, shell_arg) = SHELL;
        let mut command = Command::new(shell);
        command.arg(shell_arg);
        command.arg(command_str);

        if let Some(dir) = cwd {
            // Security check: ensure working directory doesn't contain ..
            if dir.contains("..") {
                return Err("Invalid working directory: contains '..'".to_string());
            }
            command.current_dir(dir);
        }

        // Execute command with timeout
        let output = timeout(Duration::from_secs(timeout_secs), command.output())
            .await
            .map_err(|_| "Command timed out".to_string())?
            .map_err(|e| format!("Failed to execute command: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);
        let success = output.status.success();

        Ok(CommandResult {
            stdout,
            stderr,
            exit_code,
            success,
        })
    }

    /// Internal implementation for executing commands directly
    pub async fn execute(
        cmd: &str,
        args: Vec<String>,
        cwd: Option<&str>,
        timeout_secs: u64,
    ) -> Result<CommandResult, String> {
        // Security check: block dangerous commands
        let dangerous_commands = ["rm -rf /", "rm -rf /*", "> /dev/sda", "dd if=/dev/zero"];
        let full_cmd = format!("{} {}", cmd, args.join(" "));
        for dangerous in &dangerous_commands {
            if full_cmd.contains(dangerous) {
                return Err(format!("Dangerous command blocked: {}", dangerous));
            }
        }

        let mut command = Command::new(cmd);
        command.args(&args);

        if let Some(dir) = cwd {
            // Security check: ensure working directory doesn't contain ..
            if dir.contains("..") {
                return Err("Invalid working directory: contains '..'".to_string());
            }
            command.current_dir(dir);
        }

        // Execute command with timeout
        let output = timeout(Duration::from_secs(timeout_secs), command.output())
            .await
            .map_err(|_| "Command timed out".to_string())?
            .map_err(|e| format!("Failed to execute command '{}': {}", cmd, e))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exit_code = output.status.code().unwrap_or(-1);
        let success = output.status.success();

        Ok(CommandResult {
            stdout,
            stderr,
            exit_code,
            success,
        })
    }

    /// Format command output for display
    fn format_output(&self, result: &CommandResult) -> String {
        let mut output = String::new();

        if !result.stdout.is_empty() {
            output.push_str("STDOUT:\n");
            output.push_str(&result.stdout);
            output.push('\n');
        }

        if !result.stderr.is_empty() {
            output.push_str("STDERR:\n");
            output.push_str(&result.stderr);
            output.push('\n');
        }

        output.push_str(&format!("Exit code: {}\n", result.exit_code));
        output.push_str(&format!("Success: {}\n", result.success));

        output
    }
}

impl Default for ExecuteCommandTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ExecuteCommandTool {
    fn name(&self) -> &str {
        "execute_command"
    }

    fn description(&self) -> &str {
        "Execute system commands. Supports common commands like ls, cat, pwd, echo, git, cargo, etc. Commands will timeout after 30 seconds"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "Command to execute. Can be a simple command name (e.g., 'ls', 'cat') or a full shell command with arguments (e.g., 'python3 script.py --arg1 value1'). When using shell features like pipes or redirects, use the full command string."
                },
                "args": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "List of command arguments. Optional - if omitted, the command will be executed through the system shell (sh on Unix, cmd on Windows)"
                },
                "cwd": {
                    "type": "string",
                    "description": "Working directory (optional), defaults to current directory"
                }
            },
            "required": ["command"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let command = args["command"].as_str().ok_or_else(|| {
            ToolError::InvalidArguments("Missing 'command' parameter".to_string())
        })?;

        let args_vec: Vec<String> = args["args"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let cwd = args["cwd"].as_str();

        // If args is provided, execute command directly
        // If no args, execute through shell to support full command strings
        let result = if args_vec.is_empty() {
            Self::execute_shell(command, cwd, 30).await
        } else {
            Self::execute(command, args_vec, cwd, 30).await
        };

        match result {
            Ok(result) => Ok(ToolResult {
                success: result.success,
                result: self.format_output(&result),
                display_preference: Some("markdown".to_string()),
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute_echo() {
        let tool = ExecuteCommandTool::new();
        let result = tool
            .execute(json!({"command": "echo", "args": ["Hello"]}))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("Hello"));
    }

    #[tokio::test]
    async fn test_execute_ls() {
        let tool = ExecuteCommandTool::new();
        let result = tool
            .execute(json!({"command": "ls", "args": ["/tmp"]}))
            .await
            .unwrap();

        assert!(result.success);
    }

    #[tokio::test]
    async fn test_dangerous_command_blocked() {
        let tool = ExecuteCommandTool::new();
        let result = tool
            .execute(json!({"command": "rm", "args": ["-rf", "/"]}))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("blocked"));
    }

    #[tokio::test]
    async fn test_command_not_found() {
        let tool = ExecuteCommandTool::new();
        let result = tool
            .execute(json!({"command": "nonexistent_command_xyz"}))
            .await
            .unwrap();

        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_execute_shell_command() {
        let tool = ExecuteCommandTool::new();
        // Test shell execution without args (cross-platform)
        #[cfg(not(target_os = "windows"))]
        let result = tool
            .execute(json!({"command": "echo Hello World | cat"}))
            .await
            .unwrap();

        #[cfg(target_os = "windows")]
        let result = tool
            .execute(json!({"command": "echo Hello World"}))
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.result.contains("Hello"));
    }

    #[tokio::test]
    async fn test_execute_shell_with_cwd() {
        let tool = ExecuteCommandTool::new();
        let result = tool
            .execute(json!({
                "command": "pwd",
                "cwd": "/tmp"
            }))
            .await
            .unwrap();

        assert!(result.success);
        #[cfg(not(target_os = "windows"))]
        assert!(result.result.contains("/tmp"));
    }
}
