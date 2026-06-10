use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolError, ToolExecutionContext, ToolResult};
use parking_lot::RwLock;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_MAX_RESULTS: usize = 10;
const ABSOLUTE_MAX_RESULTS: usize = 20;
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

#[derive(Debug, Deserialize)]
struct WebSearchArgs {
    query: String,
    #[serde(default)]
    allowed_domains: Option<Vec<String>>,
    #[serde(default)]
    blocked_domains: Option<Vec<String>>,
    #[serde(default)]
    max_results: Option<usize>,
}

struct CachedSearch {
    results: serde_json::Value,
    expires_at: Instant,
}

static SEARCH_CACHE: OnceLock<RwLock<HashMap<String, CachedSearch>>> = OnceLock::new();

fn search_cache() -> &'static RwLock<HashMap<String, CachedSearch>> {
    SEARCH_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

pub struct WebSearchTool;

impl WebSearchTool {
    pub fn new() -> Self {
        Self
    }

    fn cache_key(
        query: &str,
        allowed: &Option<Vec<String>>,
        blocked: &Option<Vec<String>>,
    ) -> String {
        let mut key = query.to_string();
        if let Some(domains) = allowed {
            key.push('|');
            key.push_str(&domains.join(","));
        }
        key.push('|');
        if let Some(domains) = blocked {
            key.push_str(&domains.join(","));
        }
        key
    }

    fn try_cache(key: &str) -> Option<serde_json::Value> {
        let cache = search_cache().read();
        let entry = cache.get(key)?;
        if entry.expires_at > Instant::now() {
            Some(entry.results.clone())
        } else {
            None
        }
    }

    fn put_cache(key: String, results: serde_json::Value) {
        let mut cache = search_cache().write();
        cache.insert(
            key,
            CachedSearch {
                results,
                expires_at: Instant::now() + CACHE_TTL,
            },
        );
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

    fn domain_matches(host: &str, domain: &str) -> bool {
        host == domain || host.ends_with(&format!(".{}", domain))
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
        "Search DuckDuckGo and return up to 10 filtered results (title, url, domain, snippet) with optional allow/block domain filters."
    }

    fn mutability(&self) -> crate::ToolMutability {
        crate::ToolMutability::ReadOnly
    }

    fn concurrency_safe(&self) -> bool {
        true
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
                },
                "max_results": {
                    "type": "number",
                    "description": "Maximum results to return (default 10, max 20)"
                }
            },
            "required": ["query"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("WebSearch"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: WebSearchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid WebSearch args: {}", e)))?;

        let query = parsed.query.trim();
        if query.len() < 2 {
            return Err(ToolError::InvalidArguments(
                "query must be at least 2 characters".to_string(),
            ));
        }

        let allowed_domains = parsed.allowed_domains.filter(|v| !v.is_empty());
        let blocked_domains = parsed.blocked_domains.filter(|v| !v.is_empty());

        // Mutual-exclusion validation
        if allowed_domains.is_some() && blocked_domains.is_some() {
            return Err(ToolError::InvalidArguments(
                "Cannot specify both allowed_domains and blocked_domains in the same request"
                    .to_string(),
            ));
        }

        let max_results = parsed
            .max_results
            .unwrap_or(DEFAULT_MAX_RESULTS)
            .min(ABSOLUTE_MAX_RESULTS);

        // Check cache
        let cache_key = Self::cache_key(query, &allowed_domains, &blocked_domains);
        if let Some(cached) = Self::try_cache(&cache_key) {
            ctx.emit_tool_token("Using cached search results\n").await;
            return Ok(ToolResult {
                success: true,
                result: cached.to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            });
        }

        ctx.emit_tool_token(format!("Searching: {}\n", query)).await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ToolError::Execution(format!("Failed to build HTTP client: {}", e)))?;

        let response = client
            .get("https://duckduckgo.com/html/")
            .header("User-Agent", USER_AGENT)
            .query(&[("q", query)])
            .send()
            .await
            .map_err(|e| ToolError::Execution(format!("Web search request failed: {}", e)))?;

        let html = response.text().await.map_err(|e| {
            ToolError::Execution(format!("Failed to decode web search body: {}", e))
        })?;

        // Detect anti-bot page
        if html.contains("Unfortunately, bots use DuckDuckGo too") || html.contains("anomaly-modal")
        {
            return Err(ToolError::Execution(
                "Search blocked by anti-bot protection. Please retry.".to_string(),
            ));
        }

        let allowed: Option<HashSet<String>> = allowed_domains.map(|domains| {
            domains
                .into_iter()
                .map(|value| value.to_ascii_lowercase())
                .collect()
        });
        let blocked: HashSet<String> = blocked_domains
            .unwrap_or_default()
            .into_iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();

        let link_re = Regex::new(r#"<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>"#)
            .map_err(|e| {
            ToolError::Execution(format!("Failed to compile link regex: {}", e))
        })?;
        let tag_re = Regex::new(r"(?is)<[^>]+>")
            .map_err(|e| ToolError::Execution(format!("Failed to compile tag regex: {}", e)))?;
        let snippet_re =
            Regex::new(r#"<a[^>]*class="result__snippet"[^>]*href="[^"]*"[^>]*>(.*?)</a>"#)
                .map_err(|e| {
                    ToolError::Execution(format!("Failed to compile snippet regex: {}", e))
                })?;

        // Build a map of snippet content by href (to match with result links)
        let href_re = Regex::new(r#"href="([^"]+)""#)
            .map_err(|e| ToolError::Execution(format!("Failed to compile href regex: {}", e)))?;
        let mut snippets: HashMap<String, String> = HashMap::new();
        for cap in snippet_re.captures_iter(&html) {
            if let Some(href_cap) = cap.get(0) {
                let href_text = href_cap.as_str();
                // Extract the href URL from the snippet anchor
                if let Some(url_match) = href_re.find(href_text) {
                    let raw_href = &href_text[url_match.start() + 6..url_match.end() - 1];
                    if let Some(decoded) = Self::decode_duckduckgo_url(raw_href) {
                        let snippet_text = cap
                            .get(1)
                            .map(|m| tag_re.replace_all(m.as_str(), "").trim().to_string())
                            .unwrap_or_default();
                        if !snippet_text.is_empty() {
                            snippets.insert(decoded, snippet_text);
                        }
                    }
                }
            }
        }

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

            if blocked
                .iter()
                .any(|blocked_domain| Self::domain_matches(&host, blocked_domain))
            {
                continue;
            }
            if let Some(allowed_set) = &allowed {
                if !allowed_set
                    .iter()
                    .any(|allowed_domain| Self::domain_matches(&host, allowed_domain))
                {
                    continue;
                }
            }

            let title = capture
                .get(2)
                .map(|m| tag_re.replace_all(m.as_str(), "").trim().to_string())
                .unwrap_or_else(|| url.clone());

            let snippet = snippets.get(&url).cloned().unwrap_or_default();

            let mut result = json!({
                "title": title,
                "url": url,
                "domain": host,
            });
            if !snippet.is_empty() {
                result["snippet"] = json!(snippet);
            }
            results.push(result);

            if results.len() >= max_results {
                break;
            }
        }

        ctx.emit_tool_token(format!(
            "Found {} results for \"{}\"\n",
            results.len(),
            query
        ))
        .await;

        let result_value = if results.is_empty() {
            json!({
                "query": parsed.query,
                "results": [],
                "note": "No results found for this query.",
            })
        } else {
            json!({
                "query": parsed.query,
                "results": results,
            })
        };

        // Store in cache
        Self::put_cache(cache_key, result_value.clone());

        let mut result_string = result_value.to_string();
        result_string.push_str("\n\nREMINDER: You MUST include a Sources section at the end of your response, listing all relevant URLs as markdown hyperlinks: [Title](URL)");

        Ok(ToolResult {
            success: true,
            result: result_string,
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_matches_supports_subdomains() {
        assert!(WebSearchTool::domain_matches("example.com", "example.com"));
        assert!(WebSearchTool::domain_matches(
            "docs.example.com",
            "example.com"
        ));
        assert!(!WebSearchTool::domain_matches(
            "notexample.com",
            "example.com"
        ));
        assert!(!WebSearchTool::domain_matches(
            "evil-example.com",
            "example.com"
        ));
    }

    #[test]
    fn host_of_normalizes_case() {
        let host = WebSearchTool::host_of("https://Docs.Example.Com/path").unwrap();
        assert_eq!(host, "docs.example.com");
    }

    #[test]
    fn decode_duckduckgo_url_extracts_uddg_param() {
        let raw = "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fpage&rut=whatever";
        let decoded = WebSearchTool::decode_duckduckgo_url(raw).unwrap();
        assert_eq!(decoded, "https://example.com/page");
    }

    #[test]
    fn cache_key_is_stable() {
        let k1 =
            WebSearchTool::cache_key("rust", &Some(vec!["doc.rust-lang.org".to_string()]), &None);
        let k2 =
            WebSearchTool::cache_key("rust", &Some(vec!["doc.rust-lang.org".to_string()]), &None);
        assert_eq!(k1, k2);

        let k3 = WebSearchTool::cache_key("rust", &None, &Some(vec!["bad.com".to_string()]));
        assert_ne!(k1, k3);
    }
}
