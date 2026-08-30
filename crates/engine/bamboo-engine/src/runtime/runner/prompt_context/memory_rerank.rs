use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use futures::StreamExt;
use serde::Deserialize;

use bamboo_agent_core::Message;
use bamboo_domain::ReasoningEffort;
use bamboo_llm::{LLMChunk, LLMProvider, LLMRequestOptions};
use bamboo_memory::memory_store::{
    shortlist_relevant_memories, MemoryRecallCandidate, MemoryRecallOptions, MemoryStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemoryRecallStrategy {
    Lexical,
    Reranked,
    RerankFallback,
}

impl MemoryRecallStrategy {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Lexical => "lexical",
            Self::Reranked => "reranked",
            Self::RerankFallback => "rerank_fallback",
        }
    }
}

pub(super) struct MemoryRecallSelection {
    pub(super) candidates: Vec<MemoryRecallCandidate>,
    pub(super) strategy: MemoryRecallStrategy,
}

#[derive(Clone)]
pub(super) struct MemoryRecallRerankContext {
    pub(super) llm: Arc<dyn LLMProvider>,
    pub(super) model: String,
    pub(super) session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryRecallRerankEnvelope {
    ids: Vec<String>,
}

/// Select prompt memories from the deterministic storage shortlist, optionally
/// applying the engine-owned model reranker. The storage crate remains fully
/// deterministic; provider policy and fallback semantics live at this caller.
pub(super) async fn select_relevant_memories(
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
    let candidate_limit = options.max_candidates_per_scope.max(limit);
    let candidate_options = MemoryRecallOptions {
        shortlist_limit: candidate_limit,
        include_global_fallback: options.include_global_fallback,
        max_candidates_per_scope: candidate_limit,
    };
    let mut shortlist =
        shortlist_relevant_memories(store, project_key, query, &candidate_options).await?;
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
        Ok(ids) if ids.is_empty() => Ok(MemoryRecallSelection {
            candidates: Vec::new(),
            strategy: MemoryRecallStrategy::Reranked,
        }),
        Ok(ids) => Ok(MemoryRecallSelection {
            candidates: reorder_candidates_by_ids(&shortlist, &ids, limit),
            strategy: MemoryRecallStrategy::Reranked,
        }),
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
        required_tool: None,
        responses: None,
        request_purpose: Some("memory_rerank".to_string()),
        cache: None,
    };

    let content = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut stream = context
            .llm
            .chat_stream_with_options(&messages, &[], Some(8192), model, Some(&options))
            .await
            .map_err(|error| format!("rerank provider call failed: {error}"))?;

        let mut content = String::new();
        let mut terminal_done = false;
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(LLMChunk::Token(text)) => content.push_str(&text),
                Ok(LLMChunk::Done) => {
                    terminal_done = true;
                    break;
                }
                Ok(_) => {}
                Err(error) => return Err(format!("rerank stream failed: {error}")),
            }
        }
        if !terminal_done {
            return Err("rerank stream ended without terminal completion".to_string());
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
    let explicit_empty_selection = ids.is_empty();

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

    if out.is_empty() && !explicit_empty_selection {
        return None;
    }

    Some(out)
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
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bamboo_llm::{LLMError, LLMStream};
    use bamboo_memory::memory_store::{
        DurableMemoryStatus, DurableMemoryType, MemoryScope, TemporalGranularity,
    };
    use futures::stream;

    #[derive(Clone)]
    struct StaticResponseProvider {
        response: String,
    }

    #[async_trait]
    impl LLMProvider for StaticResponseProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(self.response.clone())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[derive(Clone, Copy)]
    enum FailingProvider {
        Call,
        PendingCall,
        PendingStream,
        PartialThenError,
        EofWithoutDone,
    }

    #[async_trait]
    impl LLMProvider for FailingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            match self {
                Self::Call => Err(LLMError::Api("rerank unavailable".to_string())),
                Self::PendingCall => std::future::pending::<Result<LLMStream, LLMError>>().await,
                Self::PendingStream => Ok(Box::pin(stream::pending())),
                Self::PartialThenError => Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::Token("{\"ids\":[]}".to_string())),
                    Err(LLMError::Stream("connection reset".to_string())),
                ]))),
                Self::EofWithoutDone => Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Token(
                    "{\"ids\":[]}".to_string(),
                ))]))),
            }
        }
    }

    fn candidate(id: &str, score: f64) -> MemoryRecallCandidate {
        MemoryRecallCandidate {
            id: id.to_string(),
            title: id.to_string(),
            score,
            scope: MemoryScope::Project,
            project_key: Some("proj-1".to_string()),
            status: DurableMemoryStatus::Active,
            updated_at: "2026-04-09T00:00:00Z".to_string(),
            summary: format!("summary for {id}"),
            granularity: Some(TemporalGranularity::Month),
        }
    }

    #[test]
    fn parse_reranked_ids_accepts_fenced_json_and_filters_unknown_ids() {
        let candidates = vec![candidate("mem-a", 10.0), candidate("mem-b", 9.0)];
        let parsed = parse_reranked_ids(
            "```json\n{\"ids\":[\"mem-b\",\"unknown\",\"mem-a\",\"mem-b\"]}\n```",
            &candidates,
        )
        .expect("reranked ids should parse");

        assert_eq!(parsed, vec!["mem-b".to_string(), "mem-a".to_string()]);
    }

    #[test]
    fn parse_reranked_ids_requires_an_explicit_well_typed_ids_field() {
        let candidates = vec![candidate("mem-a", 10.0)];

        assert!(parse_reranked_ids("{}", &candidates).is_none());
        assert!(parse_reranked_ids("{\"other\":[]}", &candidates).is_none());
        assert!(parse_reranked_ids("{\"ids\":\"mem-a\"}", &candidates).is_none());
        assert!(
            parse_reranked_ids("{\"ids\":[],\"error\":\"rate limited\"}", &candidates).is_none()
        );
    }

    #[test]
    fn parse_reranked_ids_accepts_only_explicit_empty_selections() {
        let candidates = vec![candidate("mem-a", 10.0)];

        assert_eq!(
            parse_reranked_ids("{\"ids\":[]}", &candidates),
            Some(Vec::new())
        );
        assert_eq!(parse_reranked_ids("[]", &candidates), Some(Vec::new()));
        assert!(parse_reranked_ids("{\"ids\":[\"unknown\",\" \"]}", &candidates).is_none());
        assert!(parse_reranked_ids("[\"unknown\",\"\"]", &candidates).is_none());
    }

    #[test]
    fn reorder_candidates_by_ids_appends_remaining_lexical_candidates() {
        let lexical = vec![
            candidate("mem-a", 10.0),
            candidate("mem-b", 9.0),
            candidate("mem-c", 8.0),
        ];
        let reordered =
            reorder_candidates_by_ids(&lexical, &["mem-c".to_string(), "mem-a".to_string()], 3);

        assert_eq!(
            reordered
                .iter()
                .map(|candidate| candidate.id.as_str())
                .collect::<Vec<_>>(),
            vec!["mem-c", "mem-a", "mem-b"]
        );
    }

    async fn recall_store() -> (tempfile::TempDir, MemoryStore) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());
        store
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
                None,
            )
            .await
            .expect("write first memory");
        store
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
                None,
            )
            .await
            .expect("write second memory");
        (dir, store)
    }

    fn rerank_context(response: &str) -> MemoryRecallRerankContext {
        MemoryRecallRerankContext {
            llm: Arc::new(StaticResponseProvider {
                response: response.to_string(),
            }),
            model: "rerank-fast-model".to_string(),
            session_id: Some("session-1".to_string()),
        }
    }

    async fn assert_lexical_fallback(provider: Arc<dyn LLMProvider>) {
        let (_dir, store) = recall_store().await;
        let options = MemoryRecallOptions {
            shortlist_limit: 2,
            include_global_fallback: false,
            max_candidates_per_scope: 12,
        };
        let expected = shortlist_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze for mobile",
            &options,
        )
        .await
        .expect("deterministic shortlist");

        let selection = select_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze for mobile",
            &options,
            Some(&MemoryRecallRerankContext {
                llm: provider,
                model: "rerank-fast-model".to_string(),
                session_id: Some("session-1".to_string()),
            }),
        )
        .await
        .expect("fallback selection");

        assert_eq!(selection.strategy, MemoryRecallStrategy::RerankFallback);
        assert_eq!(selection.candidates, expected);
    }

    #[tokio::test]
    async fn invalid_or_empty_after_filter_response_falls_back_to_deterministic_shortlist() {
        let (_dir, store) = recall_store().await;
        let options = MemoryRecallOptions {
            shortlist_limit: 2,
            include_global_fallback: false,
            max_candidates_per_scope: 12,
        };
        let expected = shortlist_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze for mobile",
            &options,
        )
        .await
        .expect("deterministic shortlist");

        for response in [
            "not valid json",
            "{}",
            "{\"other\":[]}",
            "{\"ids\":[],\"error\":\"rate limited\"}",
            "{\"ids\":[\"unknown\",\" \"]}",
        ] {
            let selection = select_relevant_memories(
                &store,
                Some("proj-1"),
                "release freeze for mobile",
                &options,
                Some(&rerank_context(response)),
            )
            .await
            .expect("fallback selection");

            assert_eq!(selection.strategy, MemoryRecallStrategy::RerankFallback);
            assert_eq!(selection.candidates, expected);
        }
    }

    #[tokio::test]
    async fn valid_empty_model_selection_surfaces_no_memories() {
        let (_dir, store) = recall_store().await;
        for response in ["{\"ids\":[]}", "[]"] {
            let selection = select_relevant_memories(
                &store,
                Some("proj-1"),
                "release freeze for mobile",
                &MemoryRecallOptions {
                    shortlist_limit: 2,
                    include_global_fallback: false,
                    max_candidates_per_scope: 12,
                },
                Some(&rerank_context(response)),
            )
            .await
            .expect("reranked selection");

            assert_eq!(selection.strategy, MemoryRecallStrategy::Reranked);
            assert!(selection.candidates.is_empty());
        }
    }

    #[tokio::test]
    async fn provider_failure_falls_back_to_deterministic_shortlist() {
        assert_lexical_fallback(Arc::new(FailingProvider::Call)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn rerank_timeout_falls_back_to_deterministic_shortlist() {
        assert_lexical_fallback(Arc::new(FailingProvider::PendingStream)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn provider_connect_timeout_falls_back_to_deterministic_shortlist() {
        assert_lexical_fallback(Arc::new(FailingProvider::PendingCall)).await;
    }

    #[tokio::test]
    async fn partial_tokens_followed_by_stream_error_fall_back_to_deterministic_shortlist() {
        assert_lexical_fallback(Arc::new(FailingProvider::PartialThenError)).await;
    }

    #[tokio::test]
    async fn eof_without_done_falls_back_to_deterministic_shortlist() {
        assert_lexical_fallback(Arc::new(FailingProvider::EofWithoutDone)).await;
    }

    #[derive(Default)]
    struct PromptCaptureProvider {
        candidate_ids: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl LLMProvider for PromptCaptureProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let prompt = messages
                .iter()
                .rev()
                .find(|message| matches!(message.role, bamboo_agent_core::Role::User))
                .map(|message| message.content.as_str())
                .unwrap_or_default();
            let ids = prompt
                .lines()
                .filter_map(|line| {
                    let (position, id) = line.split_once(". id=")?;
                    position.trim().parse::<usize>().ok()?;
                    Some(id.trim().to_string())
                })
                .collect::<Vec<_>>();
            *self
                .candidate_ids
                .lock()
                .expect("lock should not be poisoned") = ids.clone();
            let response = serde_json::json!({ "ids": ids }).to_string();
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(response)),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[tokio::test]
    async fn rerank_sees_candidate_pool_but_final_selection_respects_shortlist_limit() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());
        for index in 0..12 {
            store
                .write_memory(
                    MemoryScope::Project,
                    Some("proj-1"),
                    DurableMemoryType::Project,
                    &format!("Release freeze component {index}"),
                    &format!(
                        "Release freeze evidence for independent component {index} with unique-marker-{index}."
                    ),
                    &[format!("component-{index}")],
                    Some("session-1"),
                    "main-model",
                    false,
                    None,
                )
                .await
                .expect("write matching memory");
        }

        let provider = Arc::new(PromptCaptureProvider::default());
        let selection = select_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze",
            &MemoryRecallOptions {
                shortlist_limit: 3,
                include_global_fallback: false,
                max_candidates_per_scope: 12,
            },
            Some(&MemoryRecallRerankContext {
                llm: provider.clone(),
                model: "rerank-fast-model".to_string(),
                session_id: Some("session-1".to_string()),
            }),
        )
        .await
        .expect("reranked selection");

        assert_eq!(selection.strategy, MemoryRecallStrategy::Reranked);
        assert_eq!(
            provider
                .candidate_ids
                .lock()
                .expect("lock should not be poisoned")
                .len(),
            12,
            "the model should see the configured rerank candidate pool"
        );
        assert_eq!(selection.candidates.len(), 3);
    }

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
                .push(options.and_then(|options| options.reasoning_effort));
            self.chat_stream(messages, tools, max_output_tokens, model)
                .await
        }
    }

    #[tokio::test]
    async fn rerank_preserves_high_reasoning_token_budget() {
        let provider = Arc::new(RequestOptionsCaptureProvider::default());
        let context = MemoryRecallRerankContext {
            llm: provider.clone(),
            model: "deepseek-v4-pro".to_string(),
            session_id: Some("test-session".to_string()),
        };

        let _ = rerank_candidate_ids("test query", &[candidate("mem-1", 0.9)], 5, &context).await;

        assert_eq!(
            provider
                .captured_reasoning
                .lock()
                .expect("lock should not be poisoned")
                .as_slice(),
            [Some(ReasoningEffort::High)]
        );
        let max_tokens = provider.captured_max_tokens.lock().expect("lock")[0]
            .expect("max_output_tokens should be set");
        assert!(max_tokens > 4096);
    }
}
