use bamboo_application_agent::{Tool, ToolError, ToolExecutionContext, ToolResult, ToolSchema};
use crate::exposure::canonical_tool_name;
use crate::guide::builtin_guides::builtin_guide_spec;
use crate::guide::ToolGuideSpec;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

const DEFAULT_LIMIT: usize = 8;
const MAX_LIMIT: usize = 50;

#[derive(Debug, Deserialize)]
struct ToolSearchArgs {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

/// Tool search — finds relevant tools by BM25 keyword relevance.
///
/// Given a free-text query, this tool scores every registered tool (built-in
/// and MCP) using a simple BM25-inspired TF–IDF ranking over name,
/// description, and parameter names.  The top-N results are returned.
///
/// Inspired by Codex's `tool_search` which uses the `bm25` crate over MCP
/// tool metadata. Our implementation is self-contained with no extra crate
/// dependency.
pub struct ToolSearchTool;

impl ToolSearchTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ToolSearchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct ToolSearchResponseItem {
    name: String,
    description: String,
    score: f64,
    category: Option<String>,
    source: String,
    when_to_use: Option<String>,
    related_tools: Vec<String>,
    parameters: Vec<String>,
}

#[async_trait]
impl Tool for ToolSearchTool {
    fn name(&self) -> &str {
        "tool_search"
    }

    fn description(&self) -> &str {
        "Search for available tools by keyword query. Returns the most relevant tools matching the query, ranked by relevance. Useful when you need to discover which tools are available for a particular task."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Free-text search query to find relevant tools"
                },
                "limit": {
                    "type": "number",
                    "description": "Maximum number of results to return (default 8, max 50)"
                }
            },
            "required": ["query"],
            "additionalProperties": false
        })
    }

    fn mutability(&self) -> crate::ToolMutability {
        crate::ToolMutability::ReadOnly
    }

    fn concurrency_safe(&self) -> bool {
        true
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: ToolSearchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid tool_search args: {}", e)))?;

        let query = parsed.query.trim().to_string();
        if query.is_empty() {
            return Err(ToolError::InvalidArguments(
                "'query' must not be empty".to_string(),
            ));
        }

        let limit = parsed.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

        Ok(ToolResult {
            success: true,
            result: json!({
                "tools": [],
                "message": "tool_search needs to be called through the executor which has access to the tool registry. Use the built-in tool system.",
                "query": query,
                "limit": limit
            })
            .to_string(),
            display_preference: None,
        })
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ToolSearchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid tool_search args: {}", e)))?;

        let query = parsed.query.trim().to_string();
        if query.is_empty() {
            return Err(ToolError::InvalidArguments(
                "'query' must not be empty".to_string(),
            ));
        }

        let limit = parsed.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        let Some(schemas) = ctx.available_tool_schemas else {
            return self.execute(json!({"query": query, "limit": limit})).await;
        };

        let docs = build_tool_docs(schemas);
        let engine = ToolSearchEngine::new(
            docs.iter()
                .map(|doc| {
                    (
                        doc.name.clone(),
                        doc.search_text.clone(),
                        doc.parameters.clone(),
                    )
                })
                .collect(),
        );
        let results = engine.search(&query, limit);
        let mut by_name = std::collections::HashMap::new();
        for doc in docs {
            by_name.insert(doc.name.clone(), doc);
        }

        let items: Vec<ToolSearchResponseItem> = results
            .into_iter()
            .filter_map(|result| {
                let doc = by_name.get(&result.name)?;
                Some(ToolSearchResponseItem {
                    name: result.name,
                    description: doc.description.clone(),
                    score: (result.score * 100.0).round() / 100.0,
                    category: doc.category.clone(),
                    source: doc.source.clone(),
                    when_to_use: doc.when_to_use.clone(),
                    related_tools: doc.related_tools.clone(),
                    parameters: doc.parameters.clone(),
                })
            })
            .collect();

        Ok(ToolResult {
            success: true,
            result: json!({
                "query": query,
                "limit": limit,
                "results": items,
            })
            .to_string(),
            display_preference: Some("json".to_string()),
        })
    }
}

// ─── BM25-lite search engine (no external crate) ───────────────────────────

#[derive(Debug, Clone)]
struct ToolSearchDocument {
    name: String,
    description: String,
    search_text: String,
    parameters: Vec<String>,
    category: Option<String>,
    source: String,
    when_to_use: Option<String>,
    related_tools: Vec<String>,
}

/// A scored search result.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub name: String,
    pub description: String,
    pub score: f64,
}

/// Simple BM25 ranking over a corpus of (name, text) documents.
pub struct ToolSearchEngine {
    docs: Vec<ToolDoc>,
    avg_dl: f64,
}

#[derive(Debug, Clone)]
struct ToolDoc {
    name: String,
    description: String,
    tokens: Vec<String>,
}

impl ToolSearchEngine {
    /// Build the search engine from (name, description, param_names) tuples.
    pub fn new(tools: Vec<(String, String, Vec<String>)>) -> Self {
        let docs: Vec<ToolDoc> = tools
            .into_iter()
            .map(|(name, description, params)| {
                let text = format!("{} {} {}", name, description, params.join(" "));
                let tokens = tokenize(&text);
                ToolDoc {
                    name,
                    description,
                    tokens,
                }
            })
            .collect();

        let total_tokens: usize = docs.iter().map(|d| d.tokens.len()).sum();
        let avg_dl = if docs.is_empty() {
            1.0
        } else {
            total_tokens as f64 / docs.len() as f64
        };

        Self { docs, avg_dl }
    }

    /// Search for the top-N most relevant tools.
    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() || self.docs.is_empty() {
            return Vec::new();
        }

        // Document frequency for each query term
        let mut df: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for token in &query_tokens {
            if df.contains_key(token.as_str()) {
                continue;
            }
            let count = self
                .docs
                .iter()
                .filter(|d| d.tokens.iter().any(|t| t == token))
                .count();
            df.insert(token, count);
        }

        let n = self.docs.len() as f64;
        let k1 = 1.2;
        let b = 0.75;

        let mut scored: Vec<(usize, f64)> = self
            .docs
            .iter()
            .enumerate()
            .map(|(idx, doc)| {
                let dl = doc.tokens.len() as f64;
                let mut score = 0.0f64;
                for qt in &query_tokens {
                    let tf = doc.tokens.iter().filter(|t| *t == qt).count() as f64;
                    let doc_freq = *df.get(qt.as_str()).unwrap_or(&0) as f64;
                    // IDF with smoothing
                    let idf = ((n - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0).ln();
                    let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / self.avg_dl));
                    score += idf * tf_norm;
                }
                (idx, score)
            })
            .collect();

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        scored
            .into_iter()
            .take(limit)
            .filter(|(_, s)| *s > 0.0)
            .map(|(idx, score)| {
                let doc = &self.docs[idx];
                SearchResult {
                    name: doc.name.clone(),
                    description: doc.description.clone(),
                    score,
                }
            })
            .collect()
    }
}

fn build_tool_docs(schemas: &[ToolSchema]) -> Vec<ToolSearchDocument> {
    let mut deduped: std::collections::BTreeMap<String, ToolSearchDocument> =
        std::collections::BTreeMap::new();

    for schema in schemas {
        let canonical = canonical_tool_name(&schema.function.name);
        let guide =
            builtin_guide_spec(&schema.function.name).or_else(|| builtin_guide_spec(&canonical));
        let parameters = parameter_names(schema);
        let category = guide.as_ref().map(|guide| format!("{:?}", guide.category));
        let when_to_use = guide.as_ref().map(|guide| guide.when_to_use.clone());
        let related_tools = guide
            .as_ref()
            .map(|guide| guide.related_tools.clone())
            .unwrap_or_default();
        let source = source_for_tool_name(&schema.function.name);
        let search_text =
            build_search_text(schema, guide.as_ref(), &parameters, &source, &canonical);

        deduped
            .entry(canonical.clone())
            .or_insert_with(|| ToolSearchDocument {
                name: canonical,
                description: schema.function.description.clone(),
                search_text,
                parameters,
                category,
                source,
                when_to_use,
                related_tools,
            });
    }

    deduped.into_values().collect()
}

fn parameter_names(schema: &ToolSchema) -> Vec<String> {
    schema
        .function
        .parameters
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .map(|properties| properties.keys().cloned().collect())
        .unwrap_or_default()
}

fn build_search_text(
    schema: &ToolSchema,
    guide: Option<&ToolGuideSpec>,
    parameters: &[String],
    source: &str,
    canonical: &str,
) -> String {
    let mut parts = vec![
        schema.function.name.clone(),
        canonical.to_string(),
        schema.function.description.clone(),
        source.to_string(),
    ];
    parts.extend(parameters.iter().cloned());

    if let Some(guide) = guide {
        parts.push(guide.when_to_use.clone());
        parts.push(guide.when_not_to_use.clone());
        parts.extend(guide.related_tools.iter().cloned());
        for example in &guide.examples {
            parts.push(example.scenario.clone());
            parts.push(example.explanation.clone());
        }
    }

    parts.join(" ")
}

fn source_for_tool_name(tool_name: &str) -> String {
    match canonical_tool_name(tool_name).as_str() {
        "scheduler"
        | "SubSession"
        | "sub_session_manager"
        | "recall"
        | "load_skill"
        | "read_skill_resource" => "server".to_string(),
        _ => "builtin".to_string(),
    }
}

/// Tokenize text: lowercase, split on non-alphanumeric, filter short tokens.
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| s.len() >= 2)
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolMutability;

    #[test]
    fn test_tool_name() {
        let tool = ToolSearchTool::new();
        assert_eq!(tool.name(), "tool_search");
    }

    #[test]
    fn test_tool_search_declares_readonly_and_parallel_safe() {
        let tool = ToolSearchTool::new();
        assert_eq!(tool.mutability(), ToolMutability::ReadOnly);
        assert!(tool.concurrency_safe());
    }

    #[tokio::test]
    async fn test_empty_query_rejected() {
        let tool = ToolSearchTool::new();
        let err = tool.execute(json!({"query": "  "})).await.unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("empty")));
    }

    #[tokio::test]
    async fn test_missing_query_rejected() {
        let tool = ToolSearchTool::new();
        let err = tool.execute(json!({})).await.unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn test_tokenize() {
        let tokens = tokenize("Hello World! foo_bar baz");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.contains(&"foo_bar".to_string()));
        assert!(tokens.contains(&"baz".to_string()));
    }

    #[test]
    fn test_search_engine_basic() {
        let tools = vec![
            (
                "Read".to_string(),
                "Read file contents from the file system".to_string(),
                vec!["file_path".to_string(), "offset".to_string()],
            ),
            (
                "Write".to_string(),
                "Write content to a file on disk".to_string(),
                vec!["file_path".to_string(), "content".to_string()],
            ),
            (
                "Bash".to_string(),
                "Execute shell commands in a terminal".to_string(),
                vec!["command".to_string()],
            ),
            (
                "WebSearch".to_string(),
                "Search the web using DuckDuckGo".to_string(),
                vec!["query".to_string()],
            ),
        ];

        let engine = ToolSearchEngine::new(tools);
        let results = engine.search("read file", 3);

        assert!(!results.is_empty());
        // "Read" should rank highest for "read file"
        assert_eq!(results[0].name, "Read");
    }

    #[test]
    fn test_search_engine_empty_corpus() {
        let engine = ToolSearchEngine::new(Vec::new());
        let results = engine.search("anything", 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_engine_empty_query() {
        let tools = vec![("Read".to_string(), "Read files".to_string(), vec![])];
        let engine = ToolSearchEngine::new(tools);
        let results = engine.search("", 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_engine_respects_limit() {
        let tools: Vec<_> = (0..20)
            .map(|i| (format!("tool_{}", i), format!("description {}", i), vec![]))
            .collect();
        let engine = ToolSearchEngine::new(tools);
        let results = engine.search("description", 3);
        assert!(results.len() <= 3);
    }

    #[test]
    fn test_search_engine_web_query() {
        let tools = vec![
            (
                "WebSearch".to_string(),
                "Search the web using DuckDuckGo".to_string(),
                vec!["query".to_string()],
            ),
            (
                "WebFetch".to_string(),
                "Fetch a URL and return cleaned text".to_string(),
                vec!["url".to_string()],
            ),
            (
                "Bash".to_string(),
                "Execute shell commands".to_string(),
                vec!["command".to_string()],
            ),
        ];

        let engine = ToolSearchEngine::new(tools);
        let results = engine.search("search web", 3);
        assert!(!results.is_empty());
        assert_eq!(results[0].name, "WebSearch");
    }

    #[tokio::test]
    async fn test_execute_with_context_returns_real_ranked_results() {
        let tool = ToolSearchTool::new();
        let schemas = vec![
            ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_application_agent::FunctionSchema {
                    name: "Read".to_string(),
                    description: "Read file contents from disk".to_string(),
                    parameters: json!({
                        "type": "object",
                        "properties": {"file_path": {"type": "string"}}
                    }),
                },
            },
            ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_application_agent::FunctionSchema {
                    name: "scheduler".to_string(),
                    description: "Manage recurring automation schedules".to_string(),
                    parameters: json!({
                        "type": "object",
                        "properties": {"action": {"type": "string"}}
                    }),
                },
            },
        ];

        let result = tool
            .execute_with_context(
                json!({"query": "read file", "limit": 2}),
                ToolExecutionContext {
                    session_id: Some("session-1"),
                    tool_call_id: "call-1",
                    event_tx: None,
                    available_tool_schemas: Some(schemas.as_slice()),
                },
            )
            .await
            .expect("tool_search with context should succeed");

        assert!(result.success);
        let value: serde_json::Value = serde_json::from_str(&result.result).expect("json result");
        let results = value["results"].as_array().expect("results array");
        assert!(!results.is_empty());
        assert_eq!(results[0]["name"], "Read");
    }
}
