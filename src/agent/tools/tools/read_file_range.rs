use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Tool for reading a specific range of lines from a file
pub struct ReadFileRangeTool;

impl ReadFileRangeTool {
    pub fn new() -> Self {
        Self
    }

    /// Read specific line range from file
    pub async fn read_file_range(
        path: &str,
        start_line: Option<usize>,
        end_line: Option<usize>,
    ) -> Result<String, String> {
        // Security check: ensure path doesn't contain ..
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let content = fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;

        let lines: Vec<&str> = content.lines().collect();
        let total_lines = lines.len();

        if total_lines == 0 {
            return Ok(String::new());
        }

        // Default to full file if no range specified
        let start = start_line.map(|s| s.saturating_sub(1)).unwrap_or(0);
        let end = end_line.map(|e| e.min(total_lines)).unwrap_or(total_lines);

        if start >= total_lines {
            return Err(format!(
                "Start line {} exceeds total lines {}",
                start + 1,
                total_lines
            ));
        }

        if start >= end {
            return Err("Start line must be less than end line".to_string());
        }

        let selected_lines = &lines[start..end];
        Ok(selected_lines.join("\n"))
    }
}

impl Default for ReadFileRangeTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ReadFileRangeTool {
    fn name(&self) -> &str {
        "read_file_range"
    }

    fn description(&self) -> &str {
        "Read a specific line range from a file. Can specify start and end lines (inclusive), line numbers start from 1"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the file"
                },
                "start_line": {
                    "type": "integer",
                    "description": "Start line number (1-based, inclusive)"
                },
                "end_line": {
                    "type": "integer",
                    "description": "End line number (inclusive); if not provided, reads to end of file"
                }
            },
            "required": ["path", "start_line"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;

        let start_line = args["start_line"].as_u64().map(|n| n as usize);
        let end_line = args["end_line"].as_u64().map(|n| n as usize);

        if start_line.is_none() {
            return Err(ToolError::InvalidArguments(
                "'start_line' must be a positive integer".to_string(),
            ));
        }

        match Self::read_file_range(path, start_line, end_line).await {
            Ok(content) => Ok(ToolResult {
                success: true,
                result: content,
                display_preference: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: Some("error".to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_file_range_tool_name() {
        let tool = ReadFileRangeTool::new();
        assert_eq!(tool.name(), "read_file_range");
    }
}
