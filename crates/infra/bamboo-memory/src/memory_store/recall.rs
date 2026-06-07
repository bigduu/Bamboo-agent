use std::cmp::Ordering;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use futures::StreamExt;
use serde::Deserialize;

use bamboo_agent_core::Message;
use bamboo_domain::ReasoningEffort;
use bamboo_llm::{LLMChunk, LLMProvider, LLMRequestOptions};

use super::{
    extract_keywords, parse_rfc3339, DurableMemoryStatus, LexicalIndexItem, MemoryScope,
    MemoryStore,
};

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecallCandidate {
    pub id: String,
    pub title: String,
    pub score: f64,
    pub scope: MemoryScope,
    pub project_key: Option<String>,
    pub status: DurableMemoryStatus,
    pub updated_at: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRecallOptions {
    pub shortlist_limit: usize,
    pub include_global_fallback: bool,
    pub max_candidates_per_scope: usize,
}

impl Default for MemoryRecallOptions {
    fn default() -> Self {
        Self {
            shortlist_limit: 3,
            include_global_fallback: true,
            max_candidates_per_scope: 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRecallStrategy {
    Lexical,
    Reranked,
    RerankFallback,
}

impl MemoryRecallStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lexical => "lexical",
            Self::Reranked => "reranked",
            Self::RerankFallback => "rerank_fallback",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecallSelection {
    pub candidates: Vec<MemoryRecallCandidate>,
    pub strategy: MemoryRecallStrategy,
}

#[derive(Clone)]
pub struct MemoryRecallRerankContext {
    pub llm: Arc<dyn LLMProvider>,
    pub model: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MemoryRecallRerankEnvelope {
    #[serde(default)]
    ids: Vec<String>,
}

pub async fn shortlist_relevant_memories(
    store: &MemoryStore,
    project_key: Option<&str>,
    query: &str,
    options: &MemoryRecallOptions,
) -> io::Result<Vec<MemoryRecallCandidate>> {
    let limit = options.shortlist_limit.max(1);
    let mut candidates =
        lexical_shortlist_relevant_memories(store, project_key, query, options).await?;
    candidates.truncate(limit);
    Ok(candidates)
}

pub async fn select_relevant_memories(
    store: &MemoryStore,
    project_key: Option<&str>,
    query: &str,
    options: &MemoryRecallOptions,
    rerank_context: Option<&MemoryRecallRerankContext>,
) -> io::Result<MemoryRecallSelection> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(MemoryRecallSelection {
            candidates: Vec::new(),
            strategy: MemoryRecallStrategy::Lexical,
        });
    }

    let limit = options.shortlist_limit.max(1);
    let mut shortlist =
        lexical_shortlist_relevant_memories(store, project_key, query, options).await?;
    if shortlist.is_empty() {
        return Ok(MemoryRecallSelection {
            candidates: shortlist,
            strategy: MemoryRecallStrategy::Lexical,
        });
    }

    let Some(rerank_context) = rerank_context else {
        shortlist.truncate(limit);
        return Ok(MemoryRecallSelection {
            candidates: shortlist,
            strategy: MemoryRecallStrategy::Lexical,
        });
    };

    if shortlist.len() <= 1 {
        shortlist.truncate(limit);
        return Ok(MemoryRecallSelection {
            candidates: shortlist,
            strategy: MemoryRecallStrategy::Lexical,
        });
    }

    match rerank_candidate_ids(query, &shortlist, limit, rerank_context).await {
        Ok(ids) => {
            let reranked = reorder_candidates_by_ids(&shortlist, &ids, limit);
            if reranked.is_empty() {
                let mut lexical = shortlist;
                lexical.truncate(limit);
                return Ok(MemoryRecallSelection {
                    candidates: lexical,
                    strategy: MemoryRecallStrategy::RerankFallback,
                });
            }
            Ok(MemoryRecallSelection {
                candidates: reranked,
                strategy: MemoryRecallStrategy::Reranked,
            })
        }
        Err(error) => {
            tracing::warn!(
                "Relevant memory rerank failed for model '{}': {}. Falling back to lexical shortlist.",
                rerank_context.model,
                error
            );
            shortlist.truncate(limit);
            Ok(MemoryRecallSelection {
                candidates: shortlist,
                strategy: MemoryRecallStrategy::RerankFallback,
            })
        }
    }
}

async fn lexical_shortlist_relevant_memories(
    store: &MemoryStore,
    project_key: Option<&str>,
    query: &str,
    options: &MemoryRecallOptions,
) -> io::Result<Vec<MemoryRecallCandidate>> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let limit = options.shortlist_limit.max(1);
    let per_scope_limit = options.max_candidates_per_scope.max(limit);

    if let Some(project_key) = project_key.map(str::trim).filter(|value| !value.is_empty()) {
        let mut project_hits =
            shortlist_scope(store, MemoryScope::Project, Some(project_key), query).await?;
        project_hits.truncate(per_scope_limit);
        if !project_hits.is_empty() {
            return Ok(project_hits);
        }
    }

    if options.include_global_fallback {
        let mut global_hits = shortlist_scope(store, MemoryScope::Global, None, query).await?;
        global_hits.truncate(per_scope_limit);
        return Ok(global_hits);
    }

    Ok(Vec::new())
}

async fn shortlist_scope(
    store: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
    query: &str,
) -> io::Result<Vec<MemoryRecallCandidate>> {
    let Some(index) = store.read_lexical_index(scope, project_key).await? else {
        return Ok(Vec::new());
    };

    let query_tokens = extract_keywords(query, "", &[]);
    if query_tokens.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates = index
        .items
        .iter()
        .filter_map(|item| score_lexical_index_item(item, &query_tokens).map(|score| (item, score)))
        .map(|(item, score)| MemoryRecallCandidate {
            id: item.id.clone(),
            title: item.title.clone(),
            score,
            scope: item.scope,
            project_key: item.project_key.clone(),
            status: item.status,
            updated_at: item.updated_at.clone(),
            summary: item.summary.clone(),
        })
        .collect::<Vec<_>>();

    sort_recall_candidates(&mut candidates);
    Ok(candidates)
}

fn score_lexical_index_item(item: &LexicalIndexItem, query_tokens: &[String]) -> Option<f64> {
    match item.status {
        DurableMemoryStatus::Superseded
        | DurableMemoryStatus::Contradicted
        | DurableMemoryStatus::Archived => return None,
        DurableMemoryStatus::Active | DurableMemoryStatus::Stale => {}
    }

    let title = item.title.to_ascii_lowercase();
    let summary = item.summary.to_ascii_lowercase();

    let mut score = 0.0;
    let mut matched_any = false;

    for token in query_tokens {
        let mut token_score = 0.0;
        if title.contains(token) {
            token_score += 3.0;
        }
        if item
            .keywords
            .iter()
            .any(|value| value.eq_ignore_ascii_case(token))
        {
            token_score += 2.5;
        }
        if item
            .tags
            .iter()
            .any(|value| value.eq_ignore_ascii_case(token))
        {
            token_score += 2.0;
        }
        if item
            .entities
            .iter()
            .any(|value| value.eq_ignore_ascii_case(token))
        {
            token_score += 1.5;
        }
        if summary.contains(token) {
            token_score += 1.0;
        }
        if token_score > 0.0 {
            matched_any = true;
            score += token_score;
        }
    }

    if !matched_any {
        return None;
    }

    score += lexical_status_adjustment(item.status);
    Some((score / query_tokens.len() as f64 * 100.0).round() / 100.0)
}

fn lexical_status_adjustment(status: DurableMemoryStatus) -> f64 {
    match status {
        DurableMemoryStatus::Active => 0.0,
        DurableMemoryStatus::Stale => -0.75,
        DurableMemoryStatus::Superseded
        | DurableMemoryStatus::Contradicted
        | DurableMemoryStatus::Archived => -10.0,
    }
}

fn sort_recall_candidates(candidates: &mut [MemoryRecallCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                let left_dt = parse_rfc3339(&left.updated_at)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
                let right_dt = parse_rfc3339(&right.updated_at)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
                right_dt.cmp(&left_dt)
            })
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn build_rerank_prompt(query: &str, candidates: &[MemoryRecallCandidate], limit: usize) -> String {
    let mut prompt = String::from("# Bamboo Relevant Memory Recall Rerank\n\n");
    prompt.push_str(
        "Select the durable memory candidates that are most relevant to the user query.\n",
    );
    prompt.push_str("Return JSON only in the form {\"ids\":[\"candidate-id\", ...]}.\n");
    prompt
        .push_str("Do not include commentary, markdown fences, explanations, or unknown ids.\n\n");
    prompt.push_str("## User query\n");
    prompt.push_str(query.trim());
    prompt.push_str("\n\n## Candidate memories\n");

    for (index, candidate) in candidates.iter().enumerate() {
        prompt.push_str(&format!(
            "{}. id={}\n   title: {}\n   scope: {}\n   status: {}\n   updated_at: {}\n   lexical_score: {:.2}\n   summary: {}\n",
            index + 1,
            candidate.id,
            candidate.title,
            candidate.scope.as_str(),
            candidate.status.as_str(),
            candidate.updated_at,
            candidate.score,
            candidate.summary.replace('\n', " "),
        ));
    }

    prompt.push_str(&format!(
        "\n## Selection rules\n- Return at most {limit} ids.\n- Use only ids from the candidate list above.\n- Prefer candidates that best answer the user query or encode active preferences/constraints relevant to it.\n- Prefer active memories over stale ones when relevance is otherwise similar.\n- Keep the ids ordered best-to-worst.\n"
    ));
    prompt
}

async fn rerank_candidate_ids(
    query: &str,
    candidates: &[MemoryRecallCandidate],
    limit: usize,
    context: &MemoryRecallRerankContext,
) -> Result<Vec<String>, String> {
    let model = context.model.trim();
    if model.is_empty() {
        return Err("rerank model is empty".to_string());
    }

    let messages = vec![
        Message::system(
            "You rerank Bamboo durable-memory recall candidates. Return strict JSON only in the form {\"ids\":[...]} using only candidate ids from the prompt.",
        ),
        Message::user(build_rerank_prompt(query, candidates, limit)),
    ];
    let options = LLMRequestOptions {
        session_id: context.session_id.clone(),
        reasoning_effort: Some(ReasoningEffort::High),
        parallel_tool_calls: None,
        responses: None,
        request_purpose: Some("memory_rerank".to_string()),
        cache: None,
    };

    let mut stream = context
        .llm
        .chat_stream_with_options(&messages, &[], Some(8192), model, Some(&options))
        .await
        .map_err(|error| format!("rerank provider call failed: {error}"))?;

    let content = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut content = String::new();
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(LLMChunk::Token(text)) => content.push_str(&text),
                Ok(LLMChunk::Done) => break,
                Ok(_) => {}
                Err(error) => {
                    if !content.trim().is_empty() {
                        break;
                    }
                    return Err(format!("rerank stream failed: {error}"));
                }
            }
        }
        Ok(content)
    })
    .await
    .unwrap_or_else(|_| Err("rerank timed out after 30s".to_string()))?;

    parse_reranked_ids(&content, candidates)
        .ok_or_else(|| format!("failed to parse rerank response: {}", content.trim()))
}

fn reorder_candidates_by_ids(
    lexical_candidates: &[MemoryRecallCandidate],
    preferred_ids: &[String],
    limit: usize,
) -> Vec<MemoryRecallCandidate> {
    if lexical_candidates.is_empty() || limit == 0 {
        return Vec::new();
    }

    let allowed = lexical_candidates
        .iter()
        .map(|candidate| candidate.id.as_str())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();

    for id in preferred_ids {
        let trimmed = id.trim();
        if trimmed.is_empty() || !allowed.contains(trimmed) || !seen.insert(trimmed.to_string()) {
            continue;
        }
        if let Some(candidate) = lexical_candidates
            .iter()
            .find(|candidate| candidate.id == trimmed)
            .cloned()
        {
            ordered.push(candidate);
            if ordered.len() >= limit {
                return ordered;
            }
        }
    }

    for candidate in lexical_candidates {
        if seen.insert(candidate.id.clone()) {
            ordered.push(candidate.clone());
            if ordered.len() >= limit {
                break;
            }
        }
    }

    ordered
}

fn parse_reranked_ids(raw: &str, candidates: &[MemoryRecallCandidate]) -> Option<Vec<String>> {
    let stripped = strip_markdown_fence(raw);
    let fragment = extract_json_fragment(&stripped).unwrap_or(stripped.trim());
    let ids = serde_json::from_str::<MemoryRecallRerankEnvelope>(fragment)
        .map(|value| value.ids)
        .or_else(|_| serde_json::from_str::<Vec<String>>(fragment))
        .ok()?;

    let allowed = candidates
        .iter()
        .map(|candidate| candidate.id.as_str())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for id in ids {
        let trimmed = id.trim();
        if trimmed.is_empty() || !allowed.contains(trimmed) || !seen.insert(trimmed.to_string()) {
            continue;
        }
        out.push(trimmed.to_string());
    }

    (!out.is_empty()).then_some(out)
}

fn strip_markdown_fence(raw: &str) -> String {
    let trimmed = raw.trim();
    for fence in ["````", "```"] {
        if let Some(after_fence) = trimmed.strip_prefix(fence) {
            let Some(first_newline) = after_fence.find('\n') else {
                continue;
            };
            let body = &after_fence[first_newline + 1..];
            if let Some(end_idx) = body.rfind(fence) {
                return body[..end_idx].trim().to_string();
            }
        }
    }
    trimmed.to_string()
}

fn extract_json_fragment(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        if start <= end {
            return Some(trimmed[start..=end].trim());
        }
    }

    if let (Some(start), Some(end)) = (trimmed.find('['), trimmed.rfind(']')) {
        if start <= end {
            return Some(trimmed[start..=end].trim());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_store::DurableMemoryType;
    use async_trait::async_trait;
    use bamboo_domain::ReasoningEffort;
    use bamboo_llm::provider::LLMRequestOptions;
    use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
    use futures::stream;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[allow(clippy::too_many_arguments)]
    fn item(
        id: &str,
        title: &str,
        status: DurableMemoryStatus,
        updated_at: &str,
        keywords: &[&str],
        tags: &[&str],
        entities: &[&str],
        summary: &str,
    ) -> LexicalIndexItem {
        LexicalIndexItem {
            id: id.to_string(),
            title: title.to_string(),
            scope: MemoryScope::Project,
            project_key: Some("proj-1".to_string()),
            r#type: DurableMemoryType::Project,
            status,
            tags: tags.iter().map(|v| v.to_string()).collect(),
            keywords: keywords.iter().map(|v| v.to_string()).collect(),
            entities: entities.iter().map(|v| v.to_string()).collect(),
            updated_at: updated_at.to_string(),
            created_at: updated_at.to_string(),
            summary: summary.to_string(),
        }
    }

    #[derive(Clone)]
    struct StaticResponseProvider {
        response: String,
        requested_models: Arc<Mutex<Vec<String>>>,
    }

    impl StaticResponseProvider {
        fn new(response: impl Into<String>) -> Self {
            Self {
                response: response.into(),
                requested_models: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl LLMProvider for StaticResponseProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.requested_models
                .lock()
                .expect("lock poisoned")
                .push(model.to_string());
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(self.response.clone())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[test]
    fn title_matches_outrank_keyword_only_matches() {
        let query_tokens = vec!["release".to_string(), "freeze".to_string()];
        let title_item = item(
            "a",
            "Release freeze decision",
            DurableMemoryStatus::Active,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );
        let keyword_item = item(
            "b",
            "Deployment decision",
            DurableMemoryStatus::Active,
            "2026-04-09T00:00:00Z",
            &["release", "freeze"],
            &[],
            &[],
            "summary",
        );

        let title_score = score_lexical_index_item(&title_item, &query_tokens).unwrap();
        let keyword_score = score_lexical_index_item(&keyword_item, &query_tokens).unwrap();
        assert!(title_score > keyword_score);
    }

    #[test]
    fn active_items_outrank_stale_items() {
        let query_tokens = vec!["release".to_string()];
        let active = item(
            "a",
            "Release freeze decision",
            DurableMemoryStatus::Active,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );
        let stale = item(
            "b",
            "Release freeze decision",
            DurableMemoryStatus::Stale,
            "2026-04-10T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );

        let active_score = score_lexical_index_item(&active, &query_tokens).unwrap();
        let stale_score = score_lexical_index_item(&stale, &query_tokens).unwrap();
        assert!(active_score > stale_score);
    }

    #[test]
    fn contradicted_and_archived_items_are_filtered_out() {
        let query_tokens = vec!["release".to_string()];
        let contradicted = item(
            "a",
            "Release freeze decision",
            DurableMemoryStatus::Contradicted,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );
        let archived = item(
            "b",
            "Release freeze decision",
            DurableMemoryStatus::Archived,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );

        assert!(score_lexical_index_item(&contradicted, &query_tokens).is_none());
        assert!(score_lexical_index_item(&archived, &query_tokens).is_none());
    }

    #[test]
    fn parse_reranked_ids_accepts_fenced_json_and_filters_unknown_ids() {
        let candidates = vec![
            MemoryRecallCandidate {
                id: "mem-a".to_string(),
                title: "A".to_string(),
                score: 10.0,
                scope: MemoryScope::Project,
                project_key: Some("proj-1".to_string()),
                status: DurableMemoryStatus::Active,
                updated_at: "2026-04-09T00:00:00Z".to_string(),
                summary: "summary a".to_string(),
            },
            MemoryRecallCandidate {
                id: "mem-b".to_string(),
                title: "B".to_string(),
                score: 9.0,
                scope: MemoryScope::Project,
                project_key: Some("proj-1".to_string()),
                status: DurableMemoryStatus::Active,
                updated_at: "2026-04-09T00:00:00Z".to_string(),
                summary: "summary b".to_string(),
            },
        ];

        let parsed = parse_reranked_ids(
            "```json\n{\"ids\":[\"mem-b\",\"unknown\",\"mem-a\",\"mem-b\"]}\n```",
            &candidates,
        )
        .expect("reranked ids should parse");

        assert_eq!(parsed, vec!["mem-b".to_string(), "mem-a".to_string()]);
    }

    #[test]
    fn reorder_candidates_by_ids_appends_remaining_lexical_candidates() {
        let lexical = vec![
            MemoryRecallCandidate {
                id: "mem-a".to_string(),
                title: "A".to_string(),
                score: 10.0,
                scope: MemoryScope::Project,
                project_key: Some("proj-1".to_string()),
                status: DurableMemoryStatus::Active,
                updated_at: "2026-04-09T00:00:00Z".to_string(),
                summary: "summary a".to_string(),
            },
            MemoryRecallCandidate {
                id: "mem-b".to_string(),
                title: "B".to_string(),
                score: 9.0,
                scope: MemoryScope::Project,
                project_key: Some("proj-1".to_string()),
                status: DurableMemoryStatus::Active,
                updated_at: "2026-04-09T00:00:00Z".to_string(),
                summary: "summary b".to_string(),
            },
            MemoryRecallCandidate {
                id: "mem-c".to_string(),
                title: "C".to_string(),
                score: 8.0,
                scope: MemoryScope::Project,
                project_key: Some("proj-1".to_string()),
                status: DurableMemoryStatus::Active,
                updated_at: "2026-04-09T00:00:00Z".to_string(),
                summary: "summary c".to_string(),
            },
        ];

        let reordered =
            reorder_candidates_by_ids(&lexical, &["mem-c".to_string(), "mem-a".to_string()], 3);

        assert_eq!(reordered[0].id, "mem-c");
        assert_eq!(reordered[1].id, "mem-a");
        assert_eq!(reordered[2].id, "mem-b");
    }

    #[tokio::test]
    async fn project_scope_shortlist_excludes_global_when_project_hits_exist() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze decision",
                "Project-specific release freeze note.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Global release guidance",
                "Global note that should not be used when project hits exist.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();

        let candidates = shortlist_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze",
            &MemoryRecallOptions::default(),
        )
        .await
        .unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.scope == MemoryScope::Project));
    }

    #[tokio::test]
    async fn global_fallback_triggers_only_when_project_hits_are_absent() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Global release guidance",
                "Fallback note for release work.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();

        let candidates = shortlist_relevant_memories(
            &store,
            Some("proj-missing"),
            "release guidance",
            &MemoryRecallOptions::default(),
        )
        .await
        .unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.scope == MemoryScope::Global));
    }

    #[tokio::test]
    async fn model_rerank_reorders_lexical_shortlist_when_enabled() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let lexical_first = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze checklist",
                "Generic release freeze checklist for shipping work.",
                &["release".to_string(), "freeze".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();
        let reranked_first = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Mobile launch blocker",
                "This durable note captures the release freeze decision for the mobile app and should be preferred for mobile freeze requests.",
                &["mobile".to_string(), "launch".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();

        let provider = StaticResponseProvider::new(format!(
            "{{\"ids\":[\"{}\",\"{}\"]}}",
            reranked_first.frontmatter.id, lexical_first.frontmatter.id
        ));
        let requested_models = provider.requested_models.clone();
        let selection = select_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze for mobile",
            &MemoryRecallOptions {
                shortlist_limit: 2,
                include_global_fallback: false,
                max_candidates_per_scope: 12,
            },
            Some(&MemoryRecallRerankContext {
                llm: Arc::new(provider),
                model: "rerank-fast-model".to_string(),
                session_id: Some("session-1".to_string()),
            }),
        )
        .await
        .unwrap();

        assert_eq!(selection.strategy, MemoryRecallStrategy::Reranked);
        assert_eq!(selection.candidates.len(), 2);
        assert_eq!(selection.candidates[0].id, reranked_first.frontmatter.id);
        assert_eq!(selection.candidates[1].id, lexical_first.frontmatter.id);
        assert_eq!(
            requested_models.lock().expect("lock poisoned").as_slice(),
            ["rerank-fast-model"]
        );
    }

    #[tokio::test]
    async fn invalid_model_rerank_response_falls_back_to_lexical_order() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let lexical_first = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze checklist",
                "Generic release freeze checklist for shipping work.",
                &["release".to_string(), "freeze".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();
        let lexical_second = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Mobile launch blocker",
                "This durable note captures the release freeze decision for the mobile app.",
                &["mobile".to_string(), "launch".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();

        let selection = select_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze for mobile",
            &MemoryRecallOptions {
                shortlist_limit: 2,
                include_global_fallback: false,
                max_candidates_per_scope: 12,
            },
            Some(&MemoryRecallRerankContext {
                llm: Arc::new(StaticResponseProvider::new("not valid json")),
                model: "rerank-fast-model".to_string(),
                session_id: Some("session-1".to_string()),
            }),
        )
        .await
        .unwrap();

        assert_eq!(selection.strategy, MemoryRecallStrategy::RerankFallback);
        assert_eq!(selection.candidates.len(), 2);
        assert_eq!(selection.candidates[0].id, lexical_first.frontmatter.id);
        assert_eq!(selection.candidates[1].id, lexical_second.frontmatter.id);
    }

    /// Provider that captures `max_output_tokens` and `reasoning_effort` from `chat_stream_with_options`.
    #[derive(Default)]
    struct RequestOptionsCaptureProvider {
        captured_max_tokens: Mutex<Vec<Option<u32>>>,
        captured_reasoning: Mutex<Vec<Option<ReasoningEffort>>>,
    }

    #[async_trait]
    impl LLMProvider for RequestOptionsCaptureProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("{\"ids\":[]}".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }

        async fn chat_stream_with_options(
            &self,
            messages: &[Message],
            tools: &[bamboo_agent_core::ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            self.captured_max_tokens
                .lock()
                .expect("lock should not be poisoned")
                .push(max_output_tokens);
            self.captured_reasoning
                .lock()
                .expect("lock should not be poisoned")
                .push(options.and_then(|o| o.reasoning_effort));
            self.chat_stream(messages, tools, max_output_tokens, model)
                .await
        }
    }

    #[tokio::test]
    async fn rerank_sufficient_max_tokens_for_high_reasoning() {
        let provider = Arc::new(RequestOptionsCaptureProvider::default());
        let candidates = vec![MemoryRecallCandidate {
            id: "mem-1".to_string(),
            score: 0.9,
            title: "Test memory".to_string(),
            scope: MemoryScope::Project,
            project_key: Some("proj-1".to_string()),
            status: DurableMemoryStatus::Active,
            updated_at: "2026-05-08T00:00:00Z".to_string(),
            summary: "A test durable memory entry".to_string(),
        }];
        let context = MemoryRecallRerankContext {
            llm: provider.clone(),
            model: "deepseek-v4-pro".to_string(),
            session_id: Some("test-session".to_string()),
        };

        let _ = rerank_candidate_ids("test query", &candidates, 5, &context).await;

        let captured_reasoning = provider
            .captured_reasoning
            .lock()
            .expect("lock should not be poisoned");
        let captured_max_tokens = provider
            .captured_max_tokens
            .lock()
            .expect("lock should not be poisoned");
        assert_eq!(
            captured_reasoning.as_slice(),
            [Some(ReasoningEffort::High)],
            "rerank should request High reasoning"
        );
        let max_tokens = captured_max_tokens[0].expect("max_output_tokens should be set");
        assert!(
            max_tokens > 4096,
            "max_output_tokens ({}) must exceed thinking budget (4096) to avoid truncation",
            max_tokens
        );
    }
}
