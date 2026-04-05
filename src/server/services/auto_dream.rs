use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::agent::core::memory_store::{MemoryScope, MemoryStore};
use crate::agent::core::storage::{SessionIndexEntry, SessionStoreV2};
use crate::agent::core::{Message, Role, SessionKind};
use crate::agent::llm::{LLMChunk, LLMProvider, LLMRequestOptions};
use crate::core::{Config, ReasoningEffort};

use super::consolidation_prompt::build_consolidation_prompt;

const DREAM_RUNTIME_SESSION_ID: &str = "__dream__";
const DREAM_INTERVAL_SECS: u64 = 60 * 30;
const DREAM_MAX_SESSIONS: usize = 12;
const DREAM_MAX_SUMMARY_CHARS: usize = 12_000;
const EXTRACTION_MAX_TOPICS_PER_SESSION: usize = 4;
const EXTRACTION_MAX_TOPIC_CHARS: usize = 1_500;
const EXTRACTION_MAX_CANDIDATES: usize = 8;

#[derive(Clone)]
pub struct AutoDreamContext {
    pub session_store: Arc<SessionStoreV2>,
    pub storage: Arc<dyn crate::agent::core::storage::Storage>,
    pub provider: Arc<dyn LLMProvider>,
    pub config: Arc<RwLock<Config>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoDreamRunResult {
    pub used_model: String,
    pub session_count: usize,
    pub note_path: std::path::PathBuf,
    pub notebook_chars: usize,
}

#[derive(Debug, Clone)]
struct CandidateSessionContext {
    entry: SessionIndexEntry,
    summary: Option<String>,
    session_id: String,
    project_key: Option<String>,
    topics: Vec<(String, String)>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct DurableExtractionEnvelope {
    #[serde(default)]
    candidates: Vec<DurableExtractionCandidate>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct DurableExtractionCandidate {
    title: String,
    #[serde(rename = "type")]
    kind: String,
    content: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    confidence: Option<String>,
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn session_is_candidate(entry: &SessionIndexEntry, since: DateTime<Utc>) -> bool {
    matches!(entry.kind, SessionKind::Root)
        && entry.updated_at >= since
        && !entry.id.trim().is_empty()
        && entry.id != DREAM_RUNTIME_SESSION_ID
}

async fn collect_candidate_sessions(
    ctx: &AutoDreamContext,
    since: DateTime<Utc>,
) -> Vec<(SessionIndexEntry, Option<String>)> {
    let mut items = ctx.session_store.list_index_entries().await;
    items.retain(|entry| session_is_candidate(entry, since));
    items.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

    let mut seen_roots = HashSet::new();
    let mut out = Vec::new();
    for entry in items.into_iter() {
        if !seen_roots.insert(entry.root_session_id.clone()) {
            continue;
        }
        let summary = match ctx.storage.load_session(&entry.id).await {
            Ok(Some(session)) => session
                .conversation_summary
                .as_ref()
                .map(|summary| summary.content.clone())
                .or_else(|| derive_session_outline(&session)),
            _ => None,
        };
        out.push((entry, summary));
        if out.len() >= DREAM_MAX_SESSIONS {
            break;
        }
    }
    out
}

async fn collect_candidate_session_contexts(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    since: DateTime<Utc>,
) -> Vec<CandidateSessionContext> {
    let sessions = collect_candidate_sessions(ctx, since).await;
    let mut out = Vec::new();
    for (entry, summary) in sessions {
        let topics = memory
            .read_session_topics_with_content(&entry.id)
            .await
            .unwrap_or_default()
            .into_iter()
            .take(EXTRACTION_MAX_TOPICS_PER_SESSION)
            .map(|(topic, content)| (topic, truncate_chars(&content, EXTRACTION_MAX_TOPIC_CHARS)))
            .collect::<Vec<_>>();
        if topics.is_empty()
            && summary
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
        {
            continue;
        }
        out.push(CandidateSessionContext {
            session_id: entry.id.clone(),
            project_key: ctx
                .storage
                .load_session(&entry.id)
                .await
                .ok()
                .flatten()
                .and_then(|session| session.metadata.get("workspace_path").cloned())
                .map(std::path::PathBuf::from)
                .map(|path| crate::agent::core::memory_store::project_key_from_path(&path))
                .or_else(|| memory.project_key_for_session(Some(&entry.id))),
            entry,
            summary,
            topics,
        });
    }
    out
}

fn build_extraction_prompt(sessions: &[CandidateSessionContext]) -> String {
    let mut prompt = String::from("# Bamboo Durable Memory Extraction\n\n");
    prompt.push_str("Extract only durable memory candidates that should become canonical project/global memory.\n\n");
    prompt.push_str("Rules:\n");
    prompt.push_str("- Return JSON only, no markdown fences or commentary unless the entire response is fenced JSON.\n");
    prompt.push_str("- Output shape: {\"candidates\":[{\"title\":string,\"type\":\"user\"|\"feedback\"|\"project\"|\"reference\",\"scope\":\"project\"|\"global\",\"content\":string,\"tags\":string[],\"session_id\":string,\"confidence\":\"high\"|\"medium\"|\"low\"}]}\n");
    prompt.push_str("- Include at most 8 candidates total.\n");
    prompt.push_str("- Skip transient scratch state, code/project structure derivable from tools, and anything low-confidence or secret-like.\n");
    prompt.push_str("- Prefer project scope when the session clearly belongs to a project workspace; otherwise use global.\n\n");
    prompt.push_str("## Candidate sessions\n\n");

    for (index, session) in sessions.iter().enumerate() {
        prompt.push_str(&format!(
            "### Session {}\n- id: {}\n- title: {}\n- project_key: {}\n- updated_at: {}\n",
            index + 1,
            session.session_id,
            session.entry.title,
            session.project_key.as_deref().unwrap_or("(none)"),
            session.entry.updated_at.to_rfc3339(),
        ));
        if let Some(summary) = session
            .summary
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            prompt.push_str("- summary:\n```md\n");
            prompt.push_str(summary);
            prompt.push_str("\n```\n");
        }
        if !session.topics.is_empty() {
            prompt.push_str("- session topics:\n");
            for (topic, content) in &session.topics {
                prompt.push_str(&format!("  - {}:\n", topic));
                prompt.push_str("    ```md\n");
                prompt.push_str(content);
                prompt.push_str("\n    ```\n");
            }
        }
        prompt.push('\n');
    }

    prompt
}

fn strip_json_fence(raw: &str) -> &str {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        return rest.trim().trim_end_matches("```").trim();
    }
    if let Some(rest) = trimmed.strip_prefix("```") {
        return rest.trim().trim_end_matches("```").trim();
    }
    trimmed
}

fn parse_extraction_candidates(raw: &str) -> Result<Vec<DurableExtractionCandidate>, String> {
    let payload = strip_json_fence(raw);
    let parsed: DurableExtractionEnvelope = serde_json::from_str(payload)
        .map_err(|error| format!("failed to parse durable extraction candidates: {error}"))?;
    Ok(parsed.candidates)
}

fn parse_candidate_scope(
    candidate: &DurableExtractionCandidate,
    project_key: Option<&str>,
) -> MemoryScope {
    match candidate
        .scope
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("project") if project_key.is_some() => MemoryScope::Project,
        Some("global") => MemoryScope::Global,
        _ if project_key.is_some() => MemoryScope::Project,
        _ => MemoryScope::Global,
    }
}

fn parse_candidate_type(kind: &str) -> Option<crate::agent::core::memory_store::DurableMemoryType> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "user" => Some(crate::agent::core::memory_store::DurableMemoryType::User),
        "feedback" => Some(crate::agent::core::memory_store::DurableMemoryType::Feedback),
        "project" => Some(crate::agent::core::memory_store::DurableMemoryType::Project),
        "reference" => Some(crate::agent::core::memory_store::DurableMemoryType::Reference),
        _ => None,
    }
}

async fn extract_and_persist_durable_candidates(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    model: &str,
    sessions: &[CandidateSessionContext],
) -> Result<usize, String> {
    if sessions.is_empty() {
        return Ok(0);
    }

    let prompt = build_extraction_prompt(sessions);
    let raw = collect_stream_text(ctx.provider.clone(), model, prompt).await?;
    let candidates = parse_extraction_candidates(&raw)?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut session_project_keys = std::collections::HashMap::new();
    for session in sessions {
        session_project_keys.insert(session.session_id.clone(), session.project_key.clone());
    }

    let extracted_at = Utc::now().to_rfc3339();
    let mut writes = 0usize;
    let mut touched_sessions = HashSet::new();
    for candidate in candidates.into_iter().take(EXTRACTION_MAX_CANDIDATES) {
        let Some(memory_type) = parse_candidate_type(&candidate.kind) else {
            continue;
        };
        let title = candidate.title.trim();
        let content = candidate.content.trim();
        if title.is_empty() || content.is_empty() {
            continue;
        }
        let session_id = candidate
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let project_key = session_id
            .and_then(|id| session_project_keys.get(id))
            .and_then(|value| value.as_deref())
            .map(ToString::to_string);
        let scope = parse_candidate_scope(&candidate, project_key.as_deref());
        let tags = candidate.tags;
        let _ = &candidate.confidence;
        memory
            .write_memory(
                scope,
                project_key.as_deref(),
                memory_type,
                title,
                content,
                &tags,
                session_id,
                "background-fast-model",
                true,
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to persist durable extraction candidate '{}': {error}",
                    title
                )
            })?;
        writes += 1;
        if let Some(session_id) = session_id {
            touched_sessions.insert(session_id.to_string());
        }
    }

    for session_id in touched_sessions {
        memory
            .mark_session_extracted(&session_id, &extracted_at)
            .await
            .map_err(|error| {
                format!("failed to update session extraction state for {session_id}: {error}")
            })?;
    }

    Ok(writes)
}

fn derive_session_outline(session: &crate::agent::core::Session) -> Option<String> {
    let mut parts = Vec::new();

    if let Some(task_list) = session.task_list.as_ref() {
        let rendered = task_list.format_for_prompt();
        if !rendered.trim().is_empty() {
            parts.push(rendered);
        }
    }

    if parts.is_empty() {
        let recent_messages = session
            .messages
            .iter()
            .rev()
            .filter(|message| !matches!(message.role, Role::System))
            .take(6)
            .collect::<Vec<_>>();
        if recent_messages.is_empty() {
            return None;
        }
        let mut rendered = String::new();
        for message in recent_messages.into_iter().rev() {
            let role = match message.role {
                Role::User => "User",
                Role::Assistant => "Assistant",
                Role::Tool => "Tool",
                Role::System => continue,
            };
            rendered.push_str(&format!(
                "**{}**: {}\n\n",
                role,
                truncate_chars(message.content.trim(), 300)
            ));
        }
        if !rendered.trim().is_empty() {
            parts.push(rendered.trim().to_string());
        }
    }

    (!parts.is_empty()).then(|| parts.join("\n\n---\n\n"))
}

async fn collect_stream_text(
    provider: Arc<dyn LLMProvider>,
    model: &str,
    prompt: String,
) -> Result<String, String> {
    let messages = vec![
        Message::system(
            "You are Bamboo's background Dream consolidator. Return only markdown notebook content."
        ),
        Message::user(prompt),
    ];
    let options = LLMRequestOptions {
        session_id: Some(DREAM_RUNTIME_SESSION_ID.to_string()),
        reasoning_effort: Some(ReasoningEffort::High),
        parallel_tool_calls: None,
        responses: None,
    };

    let mut stream = provider
        .chat_stream_with_options(&messages, &[], None, model, Some(&options))
        .await
        .map_err(|error| format!("auto-dream provider call failed: {error}"))?;

    let mut content = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(LLMChunk::Token(text)) => content.push_str(&text),
            Ok(LLMChunk::Done) => break,
            Ok(_) => {}
            Err(error) => {
                if !content.is_empty() {
                    break;
                }
                return Err(format!("auto-dream stream failed: {error}"));
            }
        }
    }

    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Err("auto-dream returned empty content".to_string());
    }
    Ok(truncate_chars(trimmed, DREAM_MAX_SUMMARY_CHARS))
}

async fn run_auto_dream_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<AutoDreamRunResult>, String> {
    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory.clone().unwrap_or_default();
    if !memory_cfg.auto_dream_enabled {
        return Ok(None);
    }

    let Some(model) = config_snapshot.get_memory_background_model() else {
        tracing::warn!(
            "[auto_dream] skipped: no memory.background_model / provider.fast_model configured"
        );
        return Ok(None);
    };

    let existing = memory
        .read_dream_view()
        .await
        .map_err(|error| format!("failed to read Dream notebook: {error}"))?;
    let since = match existing.as_deref().and_then(parse_last_consolidated_at) {
        Some(ts) => ts,
        None => Utc::now() - chrono::Duration::hours(24),
    };

    let sessions = collect_candidate_sessions(ctx, since).await;
    if sessions.is_empty() {
        return Ok(None);
    }

    let prompt = build_consolidation_prompt(&sessions);
    let notebook_body = collect_stream_text(ctx.provider.clone(), &model, prompt).await?;
    let final_note = format!(
        "# Bamboo Dream Notebook\n\nLast consolidated at: {}\nSessions reviewed: {}\nModel: {}\n\n{}\n",
        Utc::now().to_rfc3339(),
        sessions.len(),
        model,
        notebook_body.trim(),
    );

    let note_path = memory
        .write_dream_view(&final_note)
        .await
        .map_err(|error| format!("failed to persist Dream notebook: {error}"))?;

    let extraction_sessions = collect_candidate_session_contexts(ctx, memory, since).await;
    let extracted_count =
        extract_and_persist_durable_candidates(ctx, memory, &model, &extraction_sessions).await?;

    tracing::info!(
        "[auto_dream] updated Dream notebook using model '{}' from {} sessions; persisted {} durable candidates",
        model,
        sessions.len(),
        extracted_count
    );

    Ok(Some(AutoDreamRunResult {
        used_model: model,
        session_count: sessions.len(),
        note_path,
        notebook_chars: final_note.chars().count(),
    }))
}

pub async fn run_auto_dream_once(
    ctx: &AutoDreamContext,
) -> Result<Option<AutoDreamRunResult>, String> {
    let memory = MemoryStore::with_defaults();
    run_auto_dream_once_with_store(ctx, &memory).await
}

fn parse_last_consolidated_at(note: &str) -> Option<DateTime<Utc>> {
    note.lines()
        .find_map(|line| line.trim().strip_prefix("Last consolidated at: "))
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw.trim()).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn spawn_auto_dream_task(ctx: AutoDreamContext) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(DREAM_INTERVAL_SECS));
        loop {
            ticker.tick().await;
            if let Err(error) = run_auto_dream_once(&ctx).await {
                tracing::warn!("[auto_dream] run failed: {}", error);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream;

    use crate::agent::core::storage::Storage;
    use crate::agent::llm::{LLMError, LLMStream};

    #[derive(Clone)]
    struct SequenceProvider {
        responses: Arc<Mutex<Vec<String>>>,
    }

    impl SequenceProvider {
        fn new(responses: Vec<String>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
            }
        }
    }

    #[async_trait]
    impl LLMProvider for SequenceProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[crate::agent::core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let next = self.responses.lock().expect("lock poisoned").remove(0);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(next)),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[test]
    fn parse_last_consolidated_at_reads_frontmatter_line() {
        let note = "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 3\n";
        let parsed = parse_last_consolidated_at(note).expect("timestamp should parse");
        assert_eq!(parsed.to_rfc3339(), "2026-04-02T16:00:00+00:00");
    }

    #[test]
    fn parse_extraction_candidates_accepts_fenced_json() {
        let raw = "```json\n{\"candidates\":[{\"title\":\"User prefers terse responses\",\"type\":\"feedback\",\"scope\":\"global\",\"content\":\"The user prefers terse responses.\",\"tags\":[\"preference\"],\"session_id\":\"session-1\"}]}\n```";
        let candidates = parse_extraction_candidates(raw).expect("candidates should parse");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].title, "User prefers terse responses");
        assert_eq!(candidates[0].kind, "feedback");
    }

    #[tokio::test]
    async fn extract_and_persist_durable_candidates_writes_memory_and_marks_session() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        crate::core::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"User prefers terse responses\",\"type\":\"feedback\",\"scope\":\"project\",\"content\":\"The user prefers terse responses and no recap.\",\"tags\":[\"preference\",\"style\"],\"session_id\":\"session-auto\",\"confidence\":\"high\"}]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(crate::core::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
            }),
            ..Config::default()
        }));

        let mut session = crate::agent::core::Session::new("session-auto", "model");
        session.title = "Auto memory test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            temp_dir
                .path()
                .join("workspace-a")
                .to_string_lossy()
                .to_string(),
        );
        session.conversation_summary = Some(crate::agent::core::ConversationSummary::new(
            "User confirmed a stable response preference.",
            3,
            128,
        ));
        session.add_message(Message::user("Please be terse and skip the recap."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic("session-auto", "default", "User prefers terse responses.")
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store: session_store.clone(),
            storage: storage.clone(),
            provider: provider.clone(),
            config: config.clone(),
        };
        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);

        let writes =
            extract_and_persist_durable_candidates(&context, &memory, "fast-model", &contexts)
                .await
                .expect("extraction should succeed");
        assert_eq!(writes, 1);

        let project_key = crate::agent::core::memory_store::project_key_from_path(
            &temp_dir.path().join("workspace-a"),
        );
        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key),
                Some("terse recap"),
                None,
                None,
                &crate::agent::core::memory_store::MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(2000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .expect("query should succeed");
        assert_eq!(results.matched_count, 1);
        assert_eq!(results.items[0].title, "User prefers terse responses");

        let state = memory
            .read_session_state("session-auto")
            .await
            .expect("read session state");
        assert!(state.last_extracted_at.is_some());
    }

    #[tokio::test]
    async fn extract_and_persist_durable_candidates_ignores_empty_candidate_lists() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        crate::core::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> =
            Arc::new(SequenceProvider::new(vec!["{\"candidates\":[]}".to_string()]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(crate::core::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
            }),
            ..Config::default()
        }));

        let mut session = crate::agent::core::Session::new("session-empty", "model");
        session
            .metadata
            .insert("workspace_path".to_string(), temp_dir.path().to_string_lossy().to_string());
        session.add_message(Message::user("This should not produce durable memory."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic("session-empty", "default", "ephemeral scratch")
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let sessions = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        let writes = extract_and_persist_durable_candidates(&context, &memory, "fast-model", &sessions)
            .await
            .expect("empty extraction should succeed");
        assert_eq!(writes, 0);

        let state = memory
            .read_session_state("session-empty")
            .await
            .expect("read session state");
        assert!(state.last_extracted_at.is_none());
    }

    #[tokio::test]
    async fn run_auto_dream_once_updates_dream_and_persists_candidates() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        crate::core::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "## Current durable context\n- Durable signal found\n\n## Cross-session patterns\n- Prefer concise answers\n\n## Active threads to remember\n- Memory extraction\n\n## Stable constraints and preferences\n- Terse replies\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[{\"title\":\"User prefers concise answers\",\"type\":\"feedback\",\"scope\":\"project\",\"content\":\"The user prefers concise answers and minimal recap.\",\"tags\":[\"preference\"],\"session_id\":\"session-dream-run\"}]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(crate::core::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
            }),
            ..Config::default()
        }));

        let mut session = crate::agent::core::Session::new("session-dream-run", "model");
        session.title = "Dream run test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            temp_dir
                .path()
                .join("workspace-run")
                .to_string_lossy()
                .to_string(),
        );
        session.conversation_summary = Some(crate::agent::core::ConversationSummary::new(
            "Stable user preference discussed.",
            4,
            200,
        ));
        session.add_message(Message::user("Please keep answers concise."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic(
                "session-dream-run",
                "default",
                "User prefers concise answers and minimal recap.",
            )
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("auto dream run should succeed")
            .expect("auto dream should produce output");
        assert_eq!(result.used_model, "fast-model");
        assert_eq!(result.session_count, 1);

        let dream = memory
            .read_dream_view()
            .await
            .expect("read dream view")
            .expect("dream should exist");
        assert!(dream.contains("Bamboo Dream Notebook"));
        assert!(dream.contains("Durable signal found"));

        let project_key = crate::agent::core::memory_store::project_key_from_path(
            &temp_dir.path().join("workspace-run"),
        );
        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key),
                Some("concise answers"),
                None,
                None,
                &crate::agent::core::memory_store::MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(2000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .expect("query should succeed");
        assert_eq!(results.matched_count, 1);
        assert_eq!(results.items[0].title, "User prefers concise answers");
    }

    #[tokio::test]
    async fn run_auto_dream_once_returns_none_when_disabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        crate::core::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(crate::core::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: false,
            }),
            ..Config::default()
        }));

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let result = run_auto_dream_once(&context)
            .await
            .expect("disabled auto dream should not error");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn run_auto_dream_once_returns_none_without_candidate_sessions() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        crate::core::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(crate::core::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
            }),
            ..Config::default()
        }));

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let result = run_auto_dream_once(&context)
            .await
            .expect("no candidate sessions should not error");
        assert!(result.is_none());
    }
}
