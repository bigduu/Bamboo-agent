use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use parking_lot::RwLock;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use url::Url;

const CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_MAX_RESULTS: usize = 10;
const ABSOLUTE_MAX_RESULTS: usize = 20;
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const WEB_SEARCH_ENDPOINTS_ENV: &str = "BAMBOO_WEB_SEARCH_ENDPOINTS";
const DEFAULT_WEB_SEARCH_ENDPOINTS: [&str; 2] = [
    "https://html.duckduckgo.com/html/",
    "https://lite.duckduckgo.com/lite/",
];

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchResult {
    title: String,
    url: String,
    domain: String,
    snippet: Option<String>,
}

impl SearchResult {
    fn into_json(self) -> serde_json::Value {
        let mut value = json!({
            "title": self.title,
            "url": self.url,
            "domain": self.domain,
        });
        if let Some(snippet) = self.snippet {
            value["snippet"] = json!(snippet);
        }
        value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedSearchPage {
    Results(Vec<SearchResult>),
    AntiBot,
    Unrecognized,
}

static SEARCH_CACHE: OnceLock<RwLock<HashMap<String, CachedSearch>>> = OnceLock::new();

fn search_cache() -> &'static RwLock<HashMap<String, CachedSearch>> {
    SEARCH_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

// Static, compile-time-constant patterns: compile each exactly once and reuse.
// `expect` is safe here because the patterns are hardcoded and verified valid.
static ANCHOR_RE: OnceLock<Regex> = OnceLock::new();
static TAG_RE: OnceLock<Regex> = OnceLock::new();
static HREF_RE: OnceLock<Regex> = OnceLock::new();
static CLASS_RE: OnceLock<Regex> = OnceLock::new();
static LITE_SNIPPET_RE: OnceLock<Regex> = OnceLock::new();

pub struct WebSearchTool;

impl WebSearchTool {
    pub fn new() -> Self {
        Self
    }

    fn cache_key(
        query: &str,
        allowed: &Option<Vec<String>>,
        blocked: &Option<Vec<String>>,
        max_results: usize,
        endpoints: &[Url],
    ) -> String {
        let endpoints = endpoints.iter().map(Url::as_str).collect::<Vec<_>>();
        serde_json::to_string(&(query, allowed, blocked, max_results, endpoints))
            .expect("web search cache key components are serializable")
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

    fn anchor_re() -> &'static Regex {
        ANCHOR_RE
            .get_or_init(|| Regex::new(r"(?is)<a\b([^>]*)>(.*?)</a>").expect("valid static regex"))
    }

    fn tag_re() -> &'static Regex {
        TAG_RE.get_or_init(|| Regex::new(r"(?is)<[^>]+>").expect("valid static regex"))
    }

    fn href_re() -> &'static Regex {
        HREF_RE.get_or_init(|| {
            Regex::new(r#"(?i)\bhref\s*=\s*[\"']([^\"']+)[\"']"#).expect("valid static regex")
        })
    }

    fn class_re() -> &'static Regex {
        CLASS_RE.get_or_init(|| {
            Regex::new(r#"(?i)\bclass\s*=\s*[\"']([^\"']+)[\"']"#).expect("valid static regex")
        })
    }

    fn lite_snippet_re() -> &'static Regex {
        LITE_SNIPPET_RE.get_or_init(|| {
            Regex::new(
                r#"(?is)<td\b[^>]*class\s*=\s*[\"'][^\"']*\bresult-snippet\b[^\"']*[\"'][^>]*>(.*?)</td>"#,
            )
            .expect("valid static regex")
        })
    }

    fn attr_value<'a>(attrs: &'a str, pattern: &Regex) -> Option<&'a str> {
        pattern
            .captures(attrs)
            .and_then(|capture| capture.get(1))
            .map(|value| value.as_str())
    }

    fn has_class(attrs: &str, expected: &str) -> bool {
        Self::attr_value(attrs, Self::class_re()).is_some_and(|classes| {
            classes
                .split_ascii_whitespace()
                .any(|class| class == expected)
        })
    }

    fn decode_html_entities(value: &str) -> String {
        value
            .replace("&quot;", "\"")
            .replace("&#x27;", "'")
            .replace("&#39;", "'")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&nbsp;", " ")
            .replace("&amp;", "&")
    }

    fn clean_html_text(value: &str) -> String {
        let without_tags = Self::tag_re().replace_all(value, " ");
        Self::decode_html_entities(&without_tags)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn make_absolute_url(raw: &str) -> Option<String> {
        let raw = Self::decode_html_entities(raw.trim());
        if raw.is_empty() {
            return None;
        }
        if raw.starts_with("//") {
            return Some(format!("https:{raw}"));
        }
        if raw.starts_with('/') {
            return Url::parse("https://duckduckgo.com")
                .ok()?
                .join(&raw)
                .ok()
                .map(|url| url.to_string());
        }
        Some(raw)
    }

    fn decode_duckduckgo_url(raw: &str) -> Option<String> {
        let absolute = Self::make_absolute_url(raw)?;
        let parsed = Url::parse(&absolute).ok()?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return None;
        }

        let host = parsed.host_str()?.to_ascii_lowercase();
        if Self::domain_matches(&host, "duckduckgo.com") && parsed.path() == "/l/" {
            if let Some(value) = parsed
                .query_pairs()
                .find(|(key, _)| key == "uddg")
                .map(|(_, value)| value.to_string())
            {
                let target = Self::make_absolute_url(&value)?;
                let target = Url::parse(&target).ok()?;
                if !matches!(target.scheme(), "http" | "https") || target.host_str().is_none() {
                    return None;
                }
                return Some(target.to_string());
            }
        }

        Some(parsed.to_string())
    }

    fn host_of(url: &str) -> Option<String> {
        url::Url::parse(url)
            .ok()
            .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()))
    }

    fn domain_matches(host: &str, domain: &str) -> bool {
        host == domain || host.ends_with(&format!(".{}", domain))
    }

    fn result_from_parts(
        raw_url: &str,
        title_html: &str,
        snippet_html: Option<&str>,
        allowed: &Option<HashSet<String>>,
        blocked: &HashSet<String>,
    ) -> Option<SearchResult> {
        let url = Self::decode_duckduckgo_url(raw_url)?;
        let host = Self::host_of(&url)?;

        if blocked
            .iter()
            .any(|blocked_domain| Self::domain_matches(&host, blocked_domain))
        {
            return None;
        }
        if let Some(allowed_set) = allowed {
            if !allowed_set
                .iter()
                .any(|allowed_domain| Self::domain_matches(&host, allowed_domain))
            {
                return None;
            }
        }

        let title = Self::clean_html_text(title_html);
        let snippet = snippet_html
            .map(Self::clean_html_text)
            .filter(|value| !value.is_empty());
        Some(SearchResult {
            title: if title.is_empty() { url.clone() } else { title },
            url,
            domain: host,
            snippet,
        })
    }

    fn recognized_empty_page(html: &str, layout_path: &str) -> bool {
        let lower = html.to_ascii_lowercase();
        lower.contains(layout_path)
            && (lower.contains("no results")
                || lower.contains("no-results")
                || lower.contains("result--no-result"))
    }

    fn parse_html_layout(
        html: &str,
        allowed: &Option<HashSet<String>>,
        blocked: &HashSet<String>,
        max_results: usize,
    ) -> Option<Vec<SearchResult>> {
        let mut snippets = HashMap::new();
        let mut links = Vec::new();

        for capture in Self::anchor_re().captures_iter(html) {
            let attrs = capture.get(1)?.as_str();
            let content = capture.get(2)?.as_str();
            let Some(href) = Self::attr_value(attrs, Self::href_re()) else {
                continue;
            };
            if Self::has_class(attrs, "result__snippet") {
                if let Some(url) = Self::decode_duckduckgo_url(href) {
                    snippets.insert(url, content.to_string());
                }
            } else if Self::has_class(attrs, "result__a") {
                links.push((href.to_string(), content.to_string()));
            }
        }

        if links.is_empty() {
            return Self::recognized_empty_page(html, "/html/").then(Vec::new);
        }
        if !links
            .iter()
            .any(|(raw_url, _)| Self::decode_duckduckgo_url(raw_url).is_some())
        {
            return None;
        }

        let mut results = Vec::new();
        for (raw_url, title) in links {
            let decoded = Self::decode_duckduckgo_url(&raw_url);
            let snippet = decoded.as_ref().and_then(|url| snippets.get(url));
            if let Some(result) = Self::result_from_parts(
                &raw_url,
                &title,
                snippet.map(String::as_str),
                allowed,
                blocked,
            ) {
                results.push(result);
                if results.len() >= max_results {
                    break;
                }
            }
        }
        Some(results)
    }

    fn parse_lite_layout(
        html: &str,
        allowed: &Option<HashSet<String>>,
        blocked: &HashSet<String>,
        max_results: usize,
    ) -> Option<Vec<SearchResult>> {
        let mut links = Vec::new();
        for capture in Self::anchor_re().captures_iter(html) {
            let full = capture.get(0)?;
            let attrs = capture.get(1)?.as_str();
            if !Self::has_class(attrs, "result-link") {
                continue;
            }
            let Some(href) = Self::attr_value(attrs, Self::href_re()) else {
                continue;
            };
            links.push((
                full.start(),
                full.end(),
                href.to_string(),
                capture.get(2)?.as_str().to_string(),
            ));
        }

        if links.is_empty() {
            return Self::recognized_empty_page(html, "/lite/").then(Vec::new);
        }
        if !links
            .iter()
            .any(|(_, _, raw_url, _)| Self::decode_duckduckgo_url(raw_url).is_some())
        {
            return None;
        }

        let mut results = Vec::new();
        for (index, (_, end, raw_url, title)) in links.iter().enumerate() {
            let next_start = links
                .get(index + 1)
                .map(|(start, _, _, _)| *start)
                .unwrap_or(html.len());
            let snippet = Self::lite_snippet_re()
                .captures(&html[*end..next_start])
                .and_then(|capture| capture.get(1))
                .map(|value| value.as_str());
            if let Some(result) = Self::result_from_parts(raw_url, title, snippet, allowed, blocked)
            {
                results.push(result);
                if results.len() >= max_results {
                    break;
                }
            }
        }
        Some(results)
    }

    fn is_anti_bot_page(html: &str) -> bool {
        let lower = html.to_ascii_lowercase();
        lower.contains("unfortunately, bots use duckduckgo too")
            || lower.contains("anomaly-modal")
            || lower.contains("challenge-form")
    }

    fn parse_search_page(
        html: &str,
        allowed: &Option<HashSet<String>>,
        blocked: &HashSet<String>,
        max_results: usize,
    ) -> ParsedSearchPage {
        if Self::is_anti_bot_page(html) {
            return ParsedSearchPage::AntiBot;
        }
        if let Some(results) = Self::parse_html_layout(html, allowed, blocked, max_results) {
            return ParsedSearchPage::Results(results);
        }
        if let Some(results) = Self::parse_lite_layout(html, allowed, blocked, max_results) {
            return ParsedSearchPage::Results(results);
        }
        ParsedSearchPage::Unrecognized
    }

    fn parse_endpoint_list(override_value: Option<&str>) -> Result<Vec<Url>, String> {
        let raw_endpoints: Vec<&str> = match override_value {
            Some(value) => value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .collect(),
            None => DEFAULT_WEB_SEARCH_ENDPOINTS.to_vec(),
        };
        if raw_endpoints.is_empty() {
            return Err(format!(
                "{WEB_SEARCH_ENDPOINTS_ENV} must contain at least one endpoint"
            ));
        }

        raw_endpoints
            .into_iter()
            .enumerate()
            .map(|(index, raw)| {
                let url = Url::parse(raw).map_err(|_| {
                    format!(
                        "{WEB_SEARCH_ENDPOINTS_ENV} entry {} must be an absolute HTTP(S) URL",
                        index + 1
                    )
                })?;
                if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                    return Err(format!(
                        "{WEB_SEARCH_ENDPOINTS_ENV} entry {} must be an absolute HTTP(S) URL",
                        index + 1
                    ));
                }
                Ok(url)
            })
            .collect()
    }

    fn configured_endpoints() -> Result<Vec<Url>, String> {
        match std::env::var(WEB_SEARCH_ENDPOINTS_ENV) {
            Ok(value) => Self::parse_endpoint_list(Some(&value)),
            Err(std::env::VarError::NotPresent) => Self::parse_endpoint_list(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(format!(
                "{WEB_SEARCH_ENDPOINTS_ENV} must contain valid UTF-8"
            )),
        }
    }

    fn endpoint_label(endpoint: &Url) -> String {
        let host = endpoint.host_str().unwrap_or("unknown-host");
        let port = endpoint
            .port()
            .map(|port| format!(":{port}"))
            .unwrap_or_default();
        format!("{}://{host}{port}{}", endpoint.scheme(), endpoint.path())
    }

    fn build_http_client() -> Result<reqwest::Client, String> {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            // Configured endpoints are the complete trust boundary. Do not let a
            // remote response replay the query POST to an unvalidated redirect.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("Failed to build HTTP client: {error}"))
    }

    async fn search_endpoint_chain(
        client: &reqwest::Client,
        endpoints: &[Url],
        query: &str,
        allowed: &Option<HashSet<String>>,
        blocked: &HashSet<String>,
        max_results: usize,
    ) -> Result<Vec<SearchResult>, String> {
        let mut failures = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let label = Self::endpoint_label(endpoint);
            let response = match client
                .post(endpoint.clone())
                .header("User-Agent", USER_AGENT)
                .form(&[("q", query)])
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    failures.push(format!("{label}: request failed ({})", error.without_url()));
                    continue;
                }
            };
            if !response.status().is_success() {
                failures.push(format!("{label}: HTTP {}", response.status()));
                continue;
            }
            let html = match response.text().await {
                Ok(html) => html,
                Err(error) => {
                    failures.push(format!(
                        "{label}: response decode failed ({})",
                        error.without_url()
                    ));
                    continue;
                }
            };
            match Self::parse_search_page(&html, allowed, blocked, max_results) {
                ParsedSearchPage::Results(results) => return Ok(results),
                ParsedSearchPage::AntiBot => {
                    failures.push(format!("{label}: blocked by anti-bot protection"));
                }
                ParsedSearchPage::Unrecognized => {
                    failures.push(format!("{label}: unrecognized search response"));
                }
            }
        }

        Err(format!(
            "all configured web search endpoints failed: {}",
            failures.join("; ")
        ))
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

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL.promotable()
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

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
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
        let endpoints = Self::configured_endpoints().map_err(ToolError::Execution)?;

        // Check cache
        let cache_key = Self::cache_key(
            query,
            &allowed_domains,
            &blocked_domains,
            max_results,
            &endpoints,
        );
        if let Some(cached) = Self::try_cache(&cache_key) {
            ctx.emit_tool_token("Using cached search results\n").await;
            return Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: cached.to_string(),
                display_preference: Some("Collapsible".to_string()),
                images: Vec::new(),
            }));
        }

        ctx.emit_tool_token(format!("Searching: {}\n", query)).await;

        let client = Self::build_http_client().map_err(ToolError::Execution)?;

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
        let results = Self::search_endpoint_chain(
            &client,
            &endpoints,
            query,
            &allowed,
            &blocked,
            max_results,
        )
        .await
        .map_err(ToolError::Execution)?;

        ctx.emit_tool_token(format!(
            "Found {} results for \"{}\"\n",
            results.len(),
            query
        ))
        .await;

        let results: Vec<serde_json::Value> =
            results.into_iter().map(SearchResult::into_json).collect();

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

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: result_string,
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const HTML_FIXTURE: &str = include_str!("fixtures/web_search_html.html");
    const LITE_FIXTURE: &str = include_str!("fixtures/web_search_lite.html");
    const ANTI_BOT_FIXTURE: &str = include_str!("fixtures/web_search_antibot.html");

    fn empty_filters() -> (Option<HashSet<String>>, HashSet<String>) {
        (None, HashSet::new())
    }

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
    fn decode_duckduckgo_url_handles_root_and_protocol_relative_links() {
        let root_relative = "/l/?uddg=https%3A%2F%2Fexample.com%2Froot&amp;rut=irrelevant";
        assert_eq!(
            WebSearchTool::decode_duckduckgo_url(root_relative).as_deref(),
            Some("https://example.com/root")
        );

        let protocol_relative = "//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fprotocol";
        assert_eq!(
            WebSearchTool::decode_duckduckgo_url(protocol_relative).as_deref(),
            Some("https://example.com/protocol")
        );
    }

    #[test]
    fn decode_duckduckgo_url_rejects_non_http_schemes() {
        for raw in [
            "ftp://example.com/archive",
            "file://example.com/private",
            "javascript://example.com/alert",
            "/l/?uddg=ftp%3A%2F%2Fexample.com%2Farchive",
        ] {
            assert_eq!(WebSearchTool::decode_duckduckgo_url(raw), None, "{raw}");
        }
    }

    #[test]
    fn html_fixture_parses_results_snippets_and_filters() {
        let (allowed, blocked) = empty_filters();
        let ParsedSearchPage::Results(results) =
            WebSearchTool::parse_search_page(HTML_FIXTURE, &allowed, &blocked, 10)
        else {
            panic!("HTML fixture should be recognized");
        };
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Example Guide");
        assert_eq!(results[0].url, "https://example.com/guide");
        assert_eq!(
            results[0].snippet.as_deref(),
            Some("A useful guide & reference.")
        );

        let allowed = Some(HashSet::from(["example.com".to_string()]));
        let ParsedSearchPage::Results(filtered) =
            WebSearchTool::parse_search_page(HTML_FIXTURE, &allowed, &blocked, 10)
        else {
            panic!("HTML fixture should be recognized after filtering");
        };
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].domain, "example.com");
    }

    #[test]
    fn lite_fixture_uses_lite_parser_and_block_filter() {
        let allowed = None;
        let blocked = HashSet::from(["example.org".to_string()]);
        let ParsedSearchPage::Results(results) =
            WebSearchTool::parse_search_page(LITE_FIXTURE, &allowed, &blocked, 10)
        else {
            panic!("Lite fixture should be recognized by cross-layout parsing");
        };
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].url, "https://example.net/lite");
        assert_eq!(
            results[0].snippet.as_deref(),
            Some("A compact result & summary.")
        );
    }

    #[test]
    fn anti_bot_and_unknown_pages_are_hard_endpoint_failures() {
        let (allowed, blocked) = empty_filters();
        assert_eq!(
            WebSearchTool::parse_search_page(ANTI_BOT_FIXTURE, &allowed, &blocked, 10),
            ParsedSearchPage::AntiBot
        );
        assert_eq!(
            WebSearchTool::parse_search_page(
                "<html><body>proxy login page</body></html>",
                &allowed,
                &blocked,
                10,
            ),
            ParsedSearchPage::Unrecognized
        );
    }

    #[test]
    fn recognized_empty_page_is_a_successful_empty_result() {
        let (allowed, blocked) = empty_filters();
        let empty = r#"<html><body class="body--html"><form action="/html/"></form><div class="no-results">No results found</div></body></html>"#;
        assert_eq!(
            WebSearchTool::parse_search_page(empty, &allowed, &blocked, 10),
            ParsedSearchPage::Results(Vec::new())
        );
    }

    #[test]
    fn malformed_result_links_are_unrecognized_but_filtered_http_links_are_valid() {
        let (allowed, blocked) = empty_filters();
        let malformed = r#"<html><body><form action="/html/"></form><a class="result__a" href="javascript:void(0)">Broken</a></body></html>"#;
        assert_eq!(
            WebSearchTool::parse_search_page(malformed, &allowed, &blocked, 10),
            ParsedSearchPage::Unrecognized
        );

        let allowed = Some(HashSet::from(["allowed.example".to_string()]));
        let filtered = r#"<html><body><form action="/html/"></form><a class="result__a" href="https://blocked.example/result">Filtered</a></body></html>"#;
        assert_eq!(
            WebSearchTool::parse_search_page(filtered, &allowed, &blocked, 10),
            ParsedSearchPage::Results(Vec::new())
        );
    }

    #[test]
    fn endpoint_override_preserves_order_and_rejects_invalid_entries() {
        let endpoints = WebSearchTool::parse_endpoint_list(Some(
            " https://mirror.example/search , http://127.0.0.1:8080/lite ",
        ))
        .unwrap();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].as_str(), "https://mirror.example/search");
        assert_eq!(endpoints[1].as_str(), "http://127.0.0.1:8080/lite");

        assert!(WebSearchTool::parse_endpoint_list(Some("  , ")).is_err());
        assert!(WebSearchTool::parse_endpoint_list(Some("file:///tmp/results.html")).is_err());
        assert!(WebSearchTool::parse_endpoint_list(Some("not a url")).is_err());
    }

    #[test]
    fn default_endpoint_chain_uses_html_then_lite() {
        let endpoints = WebSearchTool::parse_endpoint_list(None).unwrap();
        assert_eq!(
            endpoints.iter().map(Url::as_str).collect::<Vec<_>>(),
            DEFAULT_WEB_SEARCH_ENDPOINTS
        );
    }

    #[tokio::test]
    async fn endpoint_chain_falls_back_after_an_outage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/unavailable"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/lite"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LITE_FIXTURE))
            .expect(1)
            .mount(&server)
            .await;

        let endpoints = vec![
            Url::parse(&format!("{}/unavailable", server.uri())).unwrap(),
            Url::parse(&format!("{}/lite", server.uri())).unwrap(),
        ];
        let client = WebSearchTool::build_http_client().unwrap();
        let (allowed, blocked) = empty_filters();
        let results = WebSearchTool::search_endpoint_chain(
            &client, &endpoints, "rust", &allowed, &blocked, 10,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert_eq!(request.method.as_str(), "POST");
            assert_eq!(String::from_utf8(request.body).unwrap(), "q=rust");
        }
    }

    #[tokio::test]
    async fn endpoint_chain_falls_back_after_anti_bot_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/blocked"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ANTI_BOT_FIXTURE))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/html"))
            .respond_with(ResponseTemplate::new(200).set_body_string(HTML_FIXTURE))
            .expect(1)
            .mount(&server)
            .await;

        let endpoints = vec![
            Url::parse(&format!("{}/blocked", server.uri())).unwrap(),
            Url::parse(&format!("{}/html", server.uri())).unwrap(),
        ];
        let client = WebSearchTool::build_http_client().unwrap();
        let (allowed, blocked) = empty_filters();
        let results = WebSearchTool::search_endpoint_chain(
            &client, &endpoints, "rust", &allowed, &blocked, 10,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn endpoint_chain_falls_back_after_malformed_result_links() {
        let server = MockServer::start().await;
        let malformed = r#"<html><body><form action="/html/"></form><a class="result__a" href="javascript:void(0)">Broken</a></body></html>"#;
        Mock::given(method("POST"))
            .and(path("/malformed"))
            .respond_with(ResponseTemplate::new(200).set_body_string(malformed))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/lite"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LITE_FIXTURE))
            .expect(1)
            .mount(&server)
            .await;

        let endpoints = vec![
            Url::parse(&format!("{}/malformed", server.uri())).unwrap(),
            Url::parse(&format!("{}/lite", server.uri())).unwrap(),
        ];
        let client = WebSearchTool::build_http_client().unwrap();
        let (allowed, blocked) = empty_filters();
        let results = WebSearchTool::search_endpoint_chain(
            &client, &endpoints, "rust", &allowed, &blocked, 10,
        )
        .await
        .unwrap();

        assert_eq!(results[0].url, "https://example.net/lite");
    }

    #[tokio::test]
    async fn endpoint_chain_reports_all_hard_failures() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/unavailable"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/unknown"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not a search page"))
            .mount(&server)
            .await;

        let endpoints = vec![
            Url::parse(&format!("{}/unavailable", server.uri())).unwrap(),
            Url::parse(&format!("{}/unknown", server.uri())).unwrap(),
        ];
        let client = WebSearchTool::build_http_client().unwrap();
        let (allowed, blocked) = empty_filters();
        let error = WebSearchTool::search_endpoint_chain(
            &client, &endpoints, "rust", &allowed, &blocked, 10,
        )
        .await
        .unwrap_err();

        assert!(error.contains("all configured web search endpoints failed"));
        assert!(error.contains("HTTP 503"));
        assert!(error.contains("unrecognized search response"));
    }

    #[tokio::test]
    async fn endpoint_chain_does_not_follow_redirects_before_fallback() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("Location", format!("{}/trap", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/lite"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LITE_FIXTURE))
            .expect(1)
            .mount(&server)
            .await;

        let endpoints = vec![
            Url::parse(&format!("{}/redirect", server.uri())).unwrap(),
            Url::parse(&format!("{}/lite", server.uri())).unwrap(),
        ];
        let client = WebSearchTool::build_http_client().unwrap();
        let (allowed, blocked) = empty_filters();
        let results = WebSearchTool::search_endpoint_chain(
            &client, &endpoints, "rust", &allowed, &blocked, 10,
        )
        .await
        .unwrap();

        assert_eq!(results[0].url, "https://example.net/lite");
        let paths = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .map(|request| request.url.path().to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths, ["/redirect", "/lite"]);
    }

    #[test]
    fn cache_key_is_stable_and_isolates_result_limits_and_endpoints() {
        let endpoints = WebSearchTool::parse_endpoint_list(None).unwrap();
        let allowed = Some(vec!["doc.rust-lang.org".to_string()]);
        let k1 = WebSearchTool::cache_key("rust", &allowed, &None, 10, &endpoints);
        let k2 = WebSearchTool::cache_key("rust", &allowed, &None, 10, &endpoints);
        assert_eq!(k1, k2);

        let k3 = WebSearchTool::cache_key(
            "rust",
            &None,
            &Some(vec!["bad.com".to_string()]),
            10,
            &endpoints,
        );
        assert_ne!(k1, k3);

        let fewer_results = WebSearchTool::cache_key("rust", &allowed, &None, 1, &endpoints);
        assert_ne!(k1, fewer_results);

        let mirror =
            WebSearchTool::parse_endpoint_list(Some("https://mirror.example/search")).unwrap();
        let other_endpoint = WebSearchTool::cache_key("rust", &allowed, &None, 10, &mirror);
        assert_ne!(k1, other_endpoint);
    }
}
