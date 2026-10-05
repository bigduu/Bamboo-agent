//! Codex-style metadata search over the current host-eligible Deferred tools.
//!
//! Search-text construction and BM25 setup are adapted from OpenAI Codex at
//! 7f892275e31002f0422477c6219189284560e689, copyright 2025 OpenAI,
//! licensed under Apache-2.0. See this crate's third_party/codex/LICENSE
//! and third_party/codex/NOTICE. Eligibility, exact-first references and bounded
//! deterministic result projection are Bamboo host adaptations.

use std::collections::BTreeMap;

use bamboo_domain::{
    resolve_tool_reference_name, CapabilityLoadingClass, ClassifiedToolSchema, ToolSchema,
    MAX_DISCOVERY_QUERY_CHARS, MAX_DISCOVERY_RESULTS,
};
use bm25::{Document, Language, SearchEngine, SearchEngineBuilder};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToolSearchError {
    #[error("query must not be empty")]
    EmptyQuery,
    #[error("tool search query has {actual} characters; maximum is {maximum}")]
    QueryTooLong { actual: usize, maximum: usize },
    #[error("tool search limit must be between 1 and {MAX_DISCOVERY_RESULTS}; got {0}")]
    InvalidLimit(usize),
}

/// An immutable search snapshot, rebuilt from the current eligible registry.
/// Retains public search text and exact identities, without copying schemas.
/// Search grants no callable authority; the runner still validates results/history against its registry.
pub struct ToolSearchIndex {
    tools: Vec<String>,
    search_engine: SearchEngine<usize>,
}

impl ToolSearchIndex {
    pub fn from_resolved_catalog<'a>(
        catalog: impl IntoIterator<Item = &'a ClassifiedToolSchema>,
    ) -> Self {
        // Stable document order also makes equal-score results deterministic.
        let metadata = catalog
            .into_iter()
            .filter(|entry| entry.loading_class() == CapabilityLoadingClass::Deferred)
            .map(|entry| {
                (
                    entry.execution_name().to_string(),
                    default_tool_search_text(entry.schema()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let tools = metadata.keys().cloned().collect();
        let documents: Vec<Document<usize>> = metadata
            .into_values()
            .enumerate()
            .map(|(index, search_text)| Document::new(index, search_text))
            .collect();
        let search_engine =
            SearchEngineBuilder::<usize>::with_documents(Language::English, documents).build();
        Self {
            tools,
            search_engine,
        }
    }

    pub fn search(
        &self,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, ToolSearchError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(ToolSearchError::EmptyQuery);
        }
        let actual = query.chars().count();
        if actual > MAX_DISCOVERY_QUERY_CHARS {
            return Err(ToolSearchError::QueryTooLong {
                actual,
                maximum: MAX_DISCOVERY_QUERY_CHARS,
            });
        }
        let limit = limit.unwrap_or(MAX_DISCOVERY_RESULTS);
        if limit == 0 || limit > MAX_DISCOVERY_RESULTS {
            return Err(ToolSearchError::InvalidLimit(limit));
        }
        let reference = query
            .strip_prefix("tool:")
            .or_else(|| query.strip_prefix("tool/"))
            .unwrap_or(query);
        let exact = resolve_tool_reference_name(reference, |name| {
            self.tools.iter().any(|registered| registered == name)
        });
        // Ask BM25 for every hit before applying the bounded deterministic
        // tie-breaker; its internal equal-score ordering is not registry order.
        let mut results = self.search_engine.search(query, self.tools.len());
        results.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| self.tools[left.document.id].cmp(&self.tools[right.document.id]))
        });
        let mut selected = Vec::new();
        if let Some(name) = exact {
            selected.push(name);
        }
        for result in results {
            if selected.len() == limit {
                break;
            }
            let name = &self.tools[result.document.id];
            if !selected.contains(name) {
                selected.push(name.clone());
            }
        }
        Ok(selected)
    }
}

fn default_tool_search_text(schema: &ToolSchema) -> String {
    let mut parts = Vec::new();
    push_search_part(&mut parts, &schema.function.name);
    push_search_part(&mut parts, &schema.function.name.replace('_', " "));
    push_search_part(&mut parts, &schema.function.description);
    append_schema_search_text(&schema.function.parameters, &mut parts);
    parts.join(" ")
}

fn append_schema_search_text(schema: &Value, parts: &mut Vec<String>) {
    if let Some(description) = schema.get("description").and_then(Value::as_str) {
        push_search_part(parts, description);
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, schema) in properties {
            push_search_part(parts, name);
            append_schema_search_text(schema, parts);
        }
    }
    if let Some(items) = schema.get("items") {
        append_schema_search_text(items, parts);
    }
    if let Some(variants) = schema.get("anyOf").and_then(Value::as_array) {
        for variant in variants {
            append_schema_search_text(variant, parts);
        }
    }
}

fn push_search_part(parts: &mut Vec<String>, part: &str) {
    let part = part.trim();
    if !part.is_empty() {
        parts.push(part.to_string());
    }
}

#[cfg(test)]
#[path = "tool_search_tests.rs"]
mod tests;
