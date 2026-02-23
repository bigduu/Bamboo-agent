use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use regex::Regex;
use serde_json::json;
use tokio::fs;

/// Tool for searching text across all files in a project
pub struct SearchInProjectTool;

/// Search result in a project
#[derive(Debug)]
pub struct ProjectSearchMatch {
    pub file_path: String,
    pub line_number: usize,
    pub content: String,
}

impl SearchInProjectTool {
    pub fn new() -> Self {
        Self
    }

    /// Search for pattern in all files under directory
    pub async fn search_in_project(
        directory: &str,
        pattern: &str,
        file_extensions: Option<&[String]>,
        case_sensitive: bool,
        max_results: usize,
    ) -> Result<Vec<ProjectSearchMatch>, String> {
        // Security check
        if directory.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        // Compile regex
        let regex_flags = if case_sensitive { "" } else { "(?i)" };
        let full_pattern = format!("{}{}", regex_flags, pattern);
        let regex = Regex::new(&full_pattern)
            .map_err(|e| format!("Invalid regex pattern: {}", e))?;

        let mut matches = Vec::new();
        let mut dirs_to_scan = vec![directory.to_string()];

        while let Some(current_dir) = dirs_to_scan.pop() {
            let mut entries = fs::read_dir(&current_dir)
                .await
                .map_err(|e| format!("Failed to read directory: {}", e))?;

            while let Some(entry) = entries.next_entry().await.unwrap_or(None) {
                if matches.len() >= max_results {
                    break;
                }

                let path = entry.path();
                let file_name = path.file_name().unwrap_or_default().to_string_lossy();

                // Skip hidden files and common non-source directories
                if file_name.starts_with('.')
                    || file_name == "node_modules"
                    || file_name == "target"
                    || file_name == "dist"
                    || file_name == "build"
                {
                    continue;
                }

                if path.is_dir() {
                    dirs_to_scan.push(path.to_string_lossy().to_string());
                } else if path.is_file() {
                    // Check file extension filter
                    if let Some(extensions) = file_extensions {
                        if let Some(ext) = path.extension() {
                            let ext_str = ext.to_string_lossy().to_string();
                            if !extensions.iter().any(|e| e == &ext_str) {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }

                    // Read and search file
                    if let Ok(content) = fs::read_to_string(&path).await {
                        for (line_num, line) in content.lines().enumerate() {
                            if regex.is_match(line) {
                                let relative_path = path
                                    .strip_prefix(directory)
                                    .unwrap_or(&path)
                                    .to_string_lossy()
                                    .to_string();
                                matches.push(ProjectSearchMatch {
                                    file_path: relative_path,
                                    line_number: line_num + 1,
                                    content: line.to_string(),
                                });

                                if matches.len() >= max_results {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(matches)
    }

    /// Format search results
    fn format_results(results: &[ProjectSearchMatch], max_display: usize) -> String {
        if results.is_empty() {
            return "No matches found".to_string();
        }

        let mut output = format!("Found {} match(es):\n\n", results.len());

        for (i, m) in results.iter().take(max_display).enumerate() {
            output.push_str(&format!(
                "{}. {}:{}: {}\n",
                i + 1,
                m.file_path,
                m.line_number,
                m.content.trim()
            ));
        }

        if results.len() > max_display {
            output.push_str(&format!(
                "\n... and {} more matches",
                results.len() - max_display
            ));
        }

        output
    }
}

impl Default for SearchInProjectTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SearchInProjectTool {
    fn name(&self) -> &str {
        "search_in_project"
    }

    fn description(&self) -> &str {
        "Search for text across the entire project, supporting regular expressions. Recursively scans all files in the directory, skipping directories like node_modules, target, etc."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": "Directory path to search"
                },
                "pattern": {
                    "type": "string",
                    "description": "Search pattern, supports regular expressions"
                },
                "file_extensions": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "File extension filter, e.g., [\"rs\", \"toml\"]; if not provided, searches all files"
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Whether to be case-sensitive, default false",
                    "default": false
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return, default 50",
                    "default": 50
                }
            },
            "required": ["directory", "pattern"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let directory = args["directory"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'directory' parameter".to_string()))?;

        let pattern = args["pattern"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'pattern' parameter".to_string()))?;

        let file_extensions: Option<Vec<String>> = args["file_extensions"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect());

        let case_sensitive = args["case_sensitive"].as_bool().unwrap_or(false);
        let max_results = args["max_results"].as_u64().map(|n| n as usize).unwrap_or(50);

        let ext_slice = file_extensions.as_deref();

        match Self::search_in_project(directory, pattern, ext_slice, case_sensitive, max_results).await {
            Ok(matches) => {
                let result_text = Self::format_results(&matches, 30);
                Ok(ToolResult {
                    success: true,
                    result: result_text,
                    display_preference: None,
                })
            }
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
    fn test_search_in_project_tool_name() {
        let tool = SearchInProjectTool::new();
        assert_eq!(tool.name(), "search_in_project");
    }
}
