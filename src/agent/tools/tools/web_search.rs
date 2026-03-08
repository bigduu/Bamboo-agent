use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct WebSearchArgs {
    query: String,
    #[serde(default)]
    allowed_domains: Option<Vec<String>>,
    #[serde(default)]
    blocked_domains: Option<Vec<String>>,
}

pub struct WebSearchTool;

impl WebSearchTool {
    pub fn new() -> Self {
        Self
    }

    fn decode_duckduckgo_url(raw: &str) -> Option<String> {
        if let Ok(url) = url::Url::parse(raw) {
            if let Some(value) = url
                .query_pairs()
                .find(|(key, _)| key == "uddg")
                .map(|(_, value)| value.to_string())
            {
                return Some(value);
            }
        }

        Some(raw.to_string())
    }

    fn host_of(url: &str) -> Option<String> {
        url::Url::parse(url)
            .ok()
            .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()))
    }
}

impl Default for WebSearchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &str {
        "WebSearch"
    }

    fn description(&self) -> &str {
        "Search the web and return filtered result links"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "minLength": 2,
                    "description": "The search query to use"
                },
                "allowed_domains": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Only include results from these domains"
                },
                "blocked_domains": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Never include results from these domains"
                }
            },
            "required": ["query"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: WebSearchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid WebSearch args: {}", e)))?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ToolError::Execution(format!("Failed to build HTTP client: {}", e)))?;

        let response = client
            .get("https://duckduckgo.com/html/")
            .query(&[("q", parsed.query.trim())])
            .send()
            .await
            .map_err(|e| ToolError::Execution(format!("Web search request failed: {}", e)))?;

        let html = response.text().await.map_err(|e| {
            ToolError::Execution(format!("Failed to decode web search body: {}", e))
        })?;

        let allowed: Option<HashSet<String>> = parsed.allowed_domains.map(|domains| {
            domains
                .into_iter()
                .map(|value| value.to_ascii_lowercase())
                .collect()
        });
        let blocked: HashSet<String> = parsed
            .blocked_domains
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();

        let link_re =
            Regex::new(r#"<a[^>]*class=\"result__a\"[^>]*href=\"([^\"]+)\"[^>]*>(.*?)</a>"#)
                .map_err(|e| {
                    ToolError::Execution(format!("Failed to compile parser regex: {}", e))
                })?;
        let tag_re = Regex::new(r"(?is)<[^>]+>").unwrap();

        let mut results = Vec::new();
        for capture in link_re.captures_iter(&html) {
            let Some(raw_url) = capture.get(1).map(|m| m.as_str()) else {
                continue;
            };
            let Some(url) = Self::decode_duckduckgo_url(raw_url) else {
                continue;
            };
            let Some(host) = Self::host_of(&url) else {
                continue;
            };

            if blocked.contains(&host) {
                continue;
            }
            if let Some(allowed_set) = &allowed {
                if !allowed_set.contains(&host) {
                    continue;
                }
            }

            let title = capture
                .get(2)
                .map(|m| tag_re.replace_all(m.as_str(), "").to_string())
                .unwrap_or_else(|| url.clone());

            results.push(json!({
                "title": title,
                "url": url,
                "domain": host,
            }));

            if results.len() >= 10 {
                break;
            }
        }

        Ok(ToolResult {
            success: true,
            result: json!({
                "query": parsed.query,
                "results": results,
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}
