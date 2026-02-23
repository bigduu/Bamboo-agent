use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde_json::json;
use tokio::fs;

/// Tool for applying patches to files
/// Supports unified diff format or simple line-based replacement
pub struct ApplyPatchTool;

/// Patch operation types
#[derive(Debug)]
pub enum PatchOperation {
    /// Replace specific line range with new content
    Replace {
        start_line: usize,
        end_line: usize,
        new_content: String,
    },
    /// Insert content at specific line
    Insert {
        line: usize,
        content: String,
    },
    /// Delete specific line range
    Delete {
        start_line: usize,
        end_line: usize,
    },
}

impl ApplyPatchTool {
    pub fn new() -> Self {
        Self
    }

    /// Apply a simple line-based patch
    /// Format: "@@ -start,end +start,end @@" followed by content
    /// Or simple format with explicit parameters
    pub async fn apply_patch(
        path: &str,
        old_content: &str,
        new_content: &str,
    ) -> Result<String, String> {
        // Security check: ensure path doesn't contain ..
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let file_content = fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;

        // Try to find and replace the old content
        if !file_content.contains(old_content) {
            return Err(format!(
                "Could not find the specified content in file. Content snippet: '{}'",
                if old_content.len() > 100 {
                    &old_content[..100]
                } else {
                    old_content
                }
            ));
        }

        let updated_content = file_content.replace(old_content, new_content);

        // Write back
        fs::write(path, &updated_content)
            .await
            .map_err(|e| format!("Failed to write file '{}': {}", path, e))?;

        Ok(format!(
            "Patch applied successfully. File size changed from {} to {} bytes",
            file_content.len(),
            updated_content.len()
        ))
    }

    /// Apply line-based replacement
    pub async fn replace_lines(
        path: &str,
        start_line: usize,
        end_line: usize,
        new_content: &str,
    ) -> Result<String, String> {
        // Security check
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let file_content = fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;

        let lines: Vec<&str> = file_content.lines().collect();
        let total_lines = lines.len();

        if start_line == 0 || start_line > total_lines {
            return Err(format!(
                "Invalid start_line: {}. File has {} lines",
                start_line, total_lines
            ));
        }

        if end_line < start_line || end_line > total_lines {
            return Err(format!(
                "Invalid end_line: {}. Must be between {} and {}",
                end_line, start_line, total_lines
            ));
        }

        // Build new content
        let mut result = String::new();

        // Lines before replacement
        if start_line > 1 {
            result.push_str(&lines[..start_line - 1].join("\n"));
            result.push('\n');
        }

        // New content
        result.push_str(new_content);
        if !new_content.ends_with('\n') {
            result.push('\n');
        }

        // Lines after replacement
        if end_line < total_lines {
            result.push_str(&lines[end_line..].join("\n"));
        }

        // Write back
        fs::write(path, &result)
            .await
            .map_err(|e| format!("Failed to write file '{}': {}", path, e))?;

        let lines_replaced = end_line - start_line + 1;
        let new_lines = new_content.lines().count();

        Ok(format!(
            "Successfully replaced lines {}-{} with {} new line(s) (was {} line(s))",
            start_line, end_line, new_lines, lines_replaced
        ))
    }
}

impl Default for ApplyPatchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ApplyPatchTool {
    fn name(&self) -> &str {
        "apply_patch"
    }

    fn description(&self) -> &str {
        "Apply code patches to files. Supports two modes: 1) Content replacement (find old content and replace with new content); 2) Line range replacement (replace content within specified line numbers)"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the file"
                },
                "mode": {
                    "type": "string",
                    "enum": ["content_replace", "line_replace"],
                    "description": "Replacement mode: content_replace=find and replace, line_replace=replace by line number"
                },
                "old_content": {
                    "type": "string",
                    "description": "[content_replace mode] Old content to find"
                },
                "new_content": {
                    "type": "string",
                    "description": "New content"
                },
                "start_line": {
                    "type": "integer",
                    "description": "[line_replace mode] Start line number (1-based, inclusive)"
                },
                "end_line": {
                    "type": "integer",
                    "description": "[line_replace mode] End line number (inclusive)"
                }
            },
            "required": ["path", "mode", "new_content"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;

        let mode = args["mode"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'mode' parameter".to_string()))?;

        let new_content = args["new_content"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'new_content' parameter".to_string()))?;

        let result = match mode {
            "content_replace" => {
                let old_content = args["old_content"].as_str().ok_or_else(|| {
                    ToolError::InvalidArguments(
                        "'old_content' required for content_replace mode".to_string(),
                    )
                })?;
                Self::apply_patch(path, old_content, new_content).await
            }
            "line_replace" => {
                let start_line = args["start_line"].as_u64().map(|n| n as usize).ok_or_else(|| {
                    ToolError::InvalidArguments(
                        "'start_line' required for line_replace mode".to_string(),
                    )
                })?;
                let end_line = args["end_line"]
                    .as_u64()
                    .map(|n| n as usize)
                    .unwrap_or(start_line);
                Self::replace_lines(path, start_line, end_line, new_content).await
            }
            _ => Err(format!("Unknown mode: {}. Use 'content_replace' or 'line_replace'", mode)),
        };

        match result {
            Ok(message) => Ok(ToolResult {
                success: true,
                result: message,
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
    fn test_apply_patch_tool_name() {
        let tool = ApplyPatchTool::new();
        assert_eq!(tool.name(), "apply_patch");
    }
}
