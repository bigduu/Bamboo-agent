use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use regex::Regex;
use serde_json::json;
use tokio::fs;

/// Tool for searching text in files with regex support
pub struct SearchInFileTool;

/// Search result in a file
#[derive(Debug)]
pub struct SearchMatch {
    pub line_number: usize,
    pub content: String,
    pub match_start: usize,
    pub match_end: usize,
}

impl SearchInFileTool {
    pub fn new() -> Self {
        Self
    }

    /// Search for pattern in file
    pub async fn search_in_file(
        path: &str,
        pattern: &str,
        case_sensitive: bool,
        max_results: Option<usize>,
    ) -> Result<Vec<SearchMatch>, String> {
        // Security check: ensure path doesn't contain ..
        if path.contains("..") {
            return Err("Invalid path: contains '..'".to_string());
        }

        let content = fs::read_to_string(path)
            .await
            .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;

        // Compile regex
        let regex_flags = if case_sensitive { "" } else { "(?i)" };
        let full_pattern = format!("{}{}", regex_flags, pattern);
        let regex = Regex::new(&full_pattern)
            .map_err(|e| format!("Invalid regex pattern: {}", e))?;

        let max = max_results.unwrap_or(100);
        let mut matches = Vec::new();

        for (line_num, line) in content.lines().enumerate() {
            if matches.len() >= max {
                break;
            }

            for mat in regex.find_iter(line) {
                matches.push(SearchMatch {
                    line_number: line_num + 1, // 1-based line numbers
                    content: line.to_string(),
                    match_start: mat.start(),
                    match_end: mat.end(),
                });

                if matches.len() >= max {
                    break;
                }
            }
        }

        Ok(matches)
    }

    /// Format search results for display
    fn format_results(results: &[SearchMatch], max_display: usize) -> String {
        if results.is_empty() {
            return "No matches found".to_string();
        }

        let mut output = format!("Found {} match(es):\n\n", results.len());

        for (i, m) in results.iter().take(max_display).enumerate() {
            output.push_str(&format!(
                "{}: Line {}: {}\n",
                i + 1,
                m.line_number,
                m.content
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

impl Default for SearchInFileTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SearchInFileTool {
    fn name(&self) -> &str {
        "search_in_file"
    }

    fn description(&self) -> &str {
        "Search for text in files, supporting regular expressions. Returns matching line numbers and content"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path of the file"
                },
                "pattern": {
                    "type": "string",
                    "description": "Search pattern, supports regular expressions"
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Whether to be case-sensitive, default false",
                    "default": false
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return, default 100",
                    "default": 100
                }
            },
            "required": ["path", "pattern"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let path = args["path"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'path' parameter".to_string()))?;

        let pattern = args["pattern"]
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("Missing 'pattern' parameter".to_string()))?;

        let case_sensitive = args["case_sensitive"].as_bool().unwrap_or(false);
        let max_results = args["max_results"].as_u64().map(|n| n as usize);

        match Self::search_in_file(path, pattern, case_sensitive, max_results).await {
            Ok(matches) => {
                let result_text = Self::format_results(&matches, 20);
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
    fn test_search_in_file_tool_name() {
        let tool = SearchInFileTool::new();
        assert_eq!(tool.name(), "search_in_file");
    }
}
