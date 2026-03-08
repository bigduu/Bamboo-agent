use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct WebFetchArgs {
    url: String,
    prompt: String,
}

pub struct WebFetchTool;

impl WebFetchTool {
    pub fn new() -> Self {
        Self
    }

    fn strip_html(input: &str) -> String {
        let script_re = Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap();
        let style_re = Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap();
        let tag_re = Regex::new(r"(?is)<[^>]+>").unwrap();
        let whitespace_re = Regex::new(r"[ \t\n\r]+").unwrap();

        let without_scripts = script_re.replace_all(input, " ");
        let without_styles = style_re.replace_all(&without_scripts, " ");
        let without_tags = tag_re.replace_all(&without_styles, " ");
        whitespace_re
            .replace_all(&without_tags, " ")
            .trim()
            .to_string()
    }
}

impl Default for WebFetchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &str {
        "WebFetch"
    }

    fn description(&self) -> &str {
        "Fetch a URL and process its content with a prompt"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "format": "uri",
                    "description": "The URL to fetch"
                },
                "prompt": {
                    "type": "string",
                    "description": "Prompt describing what to extract"
                }
            },
            "required": ["url", "prompt"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: WebFetchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid WebFetch args: {}", e)))?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ToolError::Execution(format!("Failed to build HTTP client: {}", e)))?;

        let response = client
            .get(parsed.url.trim())
            .send()
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to fetch URL: {}", e)))?;

        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read response body: {}", e)))?;

        let text = Self::strip_html(&body);
        let excerpt: String = text.chars().take(20_000).collect();

        Ok(ToolResult {
            success: true,
            result: json!({
                "url": parsed.url,
                "status": status,
                "prompt": parsed.prompt,
                "content": excerpt,
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}
