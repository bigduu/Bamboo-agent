//! Auto-dream orchestration: extraction, consolidation, and background Dream generation.
//!
//! Combines pure helpers (types, parsing, prompts, normalization) with
//! infrastructure-dependent orchestration code for running Dream generation
//! against real LLM providers, storage, and session stores.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::RwLock;

use bamboo_agent_core::{Message, SessionKind};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::{LLMChunk, LLMProvider, LLMRequestOptions};
use bamboo_infrastructure::{SessionIndexEntry, SessionStoreV2};

use crate::memory_store::{MemoryScope, MemoryStore};

// ---------------------------------------------------------------------------
// Extraction types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DreamGenerationMode {
    Incremental,
    Refine,
    Rebuild,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DurableExtractionEnvelope {
    #[serde(default)]
    pub candidates: Vec<DurableExtractionCandidate>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DurableExtractionCandidate {
    pub title: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub content: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub confidence: Option<String>,
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

pub fn strip_json_fence(raw: &str) -> &str {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        return rest.trim().trim_end_matches("```").trim();
    }
    if let Some(rest) = trimmed.strip_prefix("```") {
        return rest.trim().trim_end_matches("```").trim();
    }
    trimmed
}

pub fn parse_extraction_candidates(raw: &str) -> Result<Vec<DurableExtractionCandidate>, String> {
    let payload = strip_json_fence(raw);
    let parsed: DurableExtractionEnvelope = serde_json::from_str(payload)
        .map_err(|error| format!("failed to parse durable extraction candidates: {error}"))?;
    Ok(parsed.candidates)
}

pub fn parse_candidate_scope(
    candidate: &DurableExtractionCandidate,
    project_key: Option<&str>,
) -> crate::memory_store::MemoryScope {
    match candidate
        .scope
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("project") if project_key.is_some() => crate::memory_store::MemoryScope::Project,
        Some("global") => crate::memory_store::MemoryScope::Global,
        _ if project_key.is_some() => crate::memory_store::MemoryScope::Project,
        _ => crate::memory_store::MemoryScope::Global,
    }
}

pub fn parse_candidate_type(kind: &str) -> Option<crate::memory_store::DurableMemoryType> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "user" => Some(crate::memory_store::DurableMemoryType::User),
        "feedback" => Some(crate::memory_store::DurableMemoryType::Feedback),
        "project" => Some(crate::memory_store::DurableMemoryType::Project),
        "reference" => Some(crate::memory_store::DurableMemoryType::Reference),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Normalization helpers
// ---------------------------------------------------------------------------

pub fn truncate_chars(value: &str, max_chars: usize) -> String {
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

pub fn strip_markdown_fence(raw: &str) -> &str {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("```markdown") {
        return rest.trim().trim_end_matches("```").trim();
    }
    if let Some(rest) = trimmed.strip_prefix("```md") {
        return rest.trim().trim_end_matches("```").trim();
    }
    if let Some(rest) = trimmed.strip_prefix("```") {
        return rest.trim().trim_end_matches("```").trim();
    }
    trimmed
}

pub fn strip_dream_notebook_wrapper(raw: &str) -> Option<String> {
    let trimmed = strip_markdown_fence(raw).trim();
    let mut lines = trimmed.lines();
    if lines.next()?.trim() != "# Bamboo Dream Notebook" {
        return None;
    }

    let mut body_lines = Vec::new();
    let mut in_body = false;
    for line in lines {
        let trimmed_line = line.trim();
        if !in_body {
            if trimmed_line.is_empty() {
                continue;
            }
            if trimmed_line.starts_with("Project key: ")
                || trimmed_line.starts_with("Last consolidated at: ")
                || trimmed_line.starts_with("Sessions reviewed: ")
                || trimmed_line.starts_with("Model: ")
            {
                continue;
            }
            in_body = true;
        }
        body_lines.push(line);
    }

    let body = body_lines.join("\n").trim().to_string();
    (!body.is_empty()).then_some(body)
}

pub fn normalize_dream_notebook_body(raw: &str, max_chars: usize) -> Result<String, String> {
    let mut current = raw.trim().to_string();
    if current.is_empty() {
        return Err("auto-dream returned empty content".to_string());
    }

    for _ in 0..3 {
        let stripped = strip_markdown_fence(&current).trim().to_string();
        if stripped.is_empty() {
            return Err("auto-dream returned empty content".to_string());
        }

        if let Some(body) = strip_dream_notebook_wrapper(&stripped) {
            current = body;
            continue;
        }

        current = stripped;
        break;
    }

    Ok(truncate_chars(current.trim(), max_chars))
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

/// Value type for passing session info to `build_extraction_prompt`.
///
/// Decouples the prompt builder from `SessionIndexEntry` and other
/// infrastructure types.
#[derive(Debug, Clone)]
pub struct DreamCandidateInfo {
    pub session_id: String,
    pub title: String,
    pub project_key: Option<String>,
    pub updated_at: String,
    pub summary: Option<String>,
    pub topics: Vec<(String, String)>,
}

/// Build the durable memory extraction prompt from candidate session info.
///
/// Pure function — formats the prompt text used to extract durable memory
/// candidates from recent session activity.
pub fn build_extraction_prompt(candidates: &[DreamCandidateInfo]) -> String {
    let mut prompt = String::from("# Bamboo Durable Memory Extraction\n\n");
    prompt.push_str("Extract only durable memory candidates that should become canonical project/global memory.\n\n");
    prompt.push_str("Rules:\n");
    prompt.push_str("- Return JSON only, no markdown fences or commentary unless the entire response is fenced JSON.\n");
    prompt.push_str("- Output shape: {\"candidates\":[{\"title\":string,\"type\":\"user\"|\"feedback\"|\"project\"|\"reference\",\"scope\":\"project\"|\"global\",\"content\":string,\"tags\":string[],\"session_id\":string,\"confidence\":\"high\"|\"medium\"|\"low\"}]}\n");
    prompt.push_str("- Include at most 8 candidates total.\n");
    prompt.push_str("- Skip transient scratch state, code/project structure derivable from tools, and anything low-confidence or secret-like.\n");
    prompt.push_str("- Prefer project scope when the session clearly belongs to a project workspace; otherwise use global.\n\n");
    prompt.push_str("## Candidate sessions\n\n");

    for (index, session) in candidates.iter().enumerate() {
        prompt.push_str(&format!(
            "### Session {}\n- id: {}\n- title: {}\n- project_key: {}\n- updated_at: {}\n",
            index + 1,
            session.session_id,
            session.title,
            session.project_key.as_deref().unwrap_or("(none)"),
            session.updated_at,
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

// ---------------------------------------------------------------------------
// Consolidation prompt builders
// ---------------------------------------------------------------------------

const MAX_INCLUDED_CONSOLIDATION_SESSIONS: usize = 12;
const MAX_CONSOLIDATION_SUMMARY_CHARS_PER_SESSION: usize = 800;

/// Value type for passing session info to consolidation prompt builders.
///
/// Decouples from `SessionIndexEntry` so the crate stays infrastructure-free.
#[derive(Debug, Clone)]
pub struct ConsolidationSessionInfo {
    pub id: String,
    pub title: String,
    pub kind: String,
    pub updated_at: String,
    pub message_count: usize,
    pub last_run_status: Option<String>,
    pub summary: Option<String>,
}

fn build_consolidation_prompt_prefix() -> String {
    let mut prompt = String::from("# Bamboo Dream Consolidation\n\n");
    prompt
        .push_str("You are performing a lightweight reflective consolidation pass for Bamboo.\n\n");
    prompt.push_str(
        "Your job is to synthesize durable cross-session signal from recent session activity into a concise notebook entry for future work.\n\n"
    );
    prompt.push_str("Requirements:\n");
    prompt.push_str("- Focus on durable facts, recurring goals, stable constraints, user preferences, active project directions, and unresolved blockers\n");
    prompt.push_str("- Prefer cross-session patterns over one-off chatter\n");
    prompt.push_str("- Do not include secrets, tokens, or highly transient details\n");
    prompt.push_str("- Separate active ongoing threads from completed or obsolete items\n");
    prompt.push_str("- Keep the final result compact and operational\n\n");
    prompt.push_str("Return markdown with these sections exactly:\n");
    prompt.push_str("1. ## Current durable context\n");
    prompt.push_str("2. ## Cross-session patterns\n");
    prompt.push_str("3. ## Active threads to remember\n");
    prompt.push_str("4. ## Stable constraints and preferences\n");
    prompt.push_str("5. ## Open risks or questions\n\n");
    prompt
}

fn append_markdown_reference_section(
    prompt: &mut String,
    heading: &str,
    content: Option<&str>,
    empty_placeholder: &str,
) {
    prompt.push_str(heading);
    prompt.push_str("\n\n");
    if let Some(content) = content.map(str::trim).filter(|value| !value.is_empty()) {
        prompt.push_str("```md\n");
        prompt.push_str(content);
        prompt.push_str("\n```\n\n");
    } else {
        prompt.push_str(empty_placeholder);
        prompt.push_str("\n\n");
    }
}

fn append_consolidation_recent_sessions_section(
    prompt: &mut String,
    sessions: &[ConsolidationSessionInfo],
) {
    prompt.push_str("## Recent sessions\n\n");
    if sessions.is_empty() {
        prompt.push_str("_(no recent sessions supplied)_\n");
        return;
    }

    for (index, session) in sessions
        .iter()
        .take(MAX_INCLUDED_CONSOLIDATION_SESSIONS)
        .enumerate()
    {
        prompt.push_str(&format!(
            "### Session {}\n- id: {}\n- title: {}\n- kind: {}\n- updated_at: {}\n- message_count: {}\n",
            index + 1,
            session.id,
            session.title,
            session.kind,
            session.updated_at,
            session.message_count,
        ));
        if let Some(status) = session
            .last_run_status
            .as_deref()
            .filter(|v| !v.trim().is_empty())
        {
            prompt.push_str(&format!("- last_run_status: {}\n", status));
        }
        if let Some(summary) = session
            .summary
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            prompt.push_str("- summary:\n```md\n");
            prompt.push_str(&truncate_chars(
                summary,
                MAX_CONSOLIDATION_SUMMARY_CHARS_PER_SESSION,
            ));
            prompt.push_str("\n```\n");
        }
        prompt.push('\n');
    }

    if sessions.len() > MAX_INCLUDED_CONSOLIDATION_SESSIONS {
        prompt.push_str(&format!(
            "_Only the most recent {} sessions are included in this pass out of {} candidates._\n",
            MAX_INCLUDED_CONSOLIDATION_SESSIONS,
            sessions.len()
        ));
    }
}

pub fn build_consolidation_prompt(sessions: &[ConsolidationSessionInfo]) -> String {
    let mut prompt = build_consolidation_prompt_prefix();
    append_consolidation_recent_sessions_section(&mut prompt, sessions);
    prompt
}

pub fn build_consolidation_prompt_with_existing_dream(
    existing_dream: Option<&str>,
    sessions: &[ConsolidationSessionInfo],
) -> String {
    build_refine_consolidation_prompt(existing_dream, None, sessions)
}

pub fn build_refine_consolidation_prompt(
    existing_dream: Option<&str>,
    recent_durable_memory: Option<&str>,
    sessions: &[ConsolidationSessionInfo],
) -> String {
    let mut prompt = build_consolidation_prompt_prefix();
    prompt.push_str(
        "When an existing Dream notebook is provided, start from it and preserve still-valid durable context while updating active threads based on recent sessions and recent durable memory updates. Remove obsolete items only when the recent evidence justifies it.\n\n",
    );
    append_markdown_reference_section(
        &mut prompt,
        "## Existing Dream notebook",
        existing_dream,
        "_(no existing Dream notebook supplied; fall back to synthesizing from recent sessions only)_",
    );
    append_markdown_reference_section(
        &mut prompt,
        "## Recent durable memory updates",
        recent_durable_memory,
        "_(no recent durable memory updates supplied)_",
    );
    append_consolidation_recent_sessions_section(&mut prompt, sessions);
    prompt
}

pub fn build_rebuild_consolidation_prompt(
    durable_memory_index: Option<&str>,
    sessions: &[ConsolidationSessionInfo],
) -> String {
    let mut prompt = build_consolidation_prompt_prefix();
    prompt.push_str(
        "You are rebuilding the Dream notebook from canonical durable memory plus recent session activity. Use the durable memory index as the primary long-lived signal, and use recent sessions to refresh active threads, current priorities, and unresolved questions.\n\n",
    );
    append_markdown_reference_section(
        &mut prompt,
        "## Durable memory index",
        durable_memory_index,
        "_(no durable memory index supplied)_",
    );
    append_consolidation_recent_sessions_section(&mut prompt, sessions);
    prompt
}

// ---------------------------------------------------------------------------
// Session outline and dream normalization
// ---------------------------------------------------------------------------

/// Derive a brief text outline from a session for dream extraction context.
///
/// Uses the task list if available, otherwise falls back to the 6 most recent
/// non-system messages (truncated to 300 chars each).
pub fn derive_session_outline(session: &bamboo_agent_core::Session) -> Option<String> {
    use bamboo_agent_core::Role;

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

/// Normalize an existing dream notebook body for use as consolidation prompt context.
///
/// Returns `None` if normalization fails (logged as a warning).
pub fn normalize_existing_dream_for_prompt(
    existing_dream: Option<&str>,
    model: &str,
    session_count: usize,
    max_summary_chars: usize,
) -> Option<String> {
    existing_dream.and_then(|dream| {
        match normalize_dream_notebook_body(dream, max_summary_chars) {
            Ok(body) => Some(body),
            Err(error) => {
                tracing::warn!(
                    target: "bamboo.auto_dream",
                    event = "existing_input_normalization_failed",
                    model = model,
                    session_count = session_count,
                    "[auto_dream] failed to normalize existing Dream input; omitting prior Dream context: {}",
                    error
                );
                None
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

pub fn should_use_dream_refine_mode(
    memory_cfg: &bamboo_infrastructure::config::MemoryConfig,
) -> bool {
    memory_cfg.dream_refine_mode
}

pub fn should_force_full_rebuild(
    last_full_rebuild_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
    rebuild_interval_secs: i64,
) -> bool {
    match last_full_rebuild_at {
        Some(timestamp) => (now - timestamp) >= chrono::Duration::seconds(rebuild_interval_secs),
        None => false,
    }
}

pub fn parse_last_full_rebuild_at(note: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    note.lines()
        .find_map(|line| line.trim().strip_prefix("Last full rebuild at: "))
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw.trim()).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

pub fn parse_last_consolidated_at(note: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    note.lines()
        .find_map(|line| line.trim().strip_prefix("Last consolidated at: "))
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw.trim()).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

// ---------------------------------------------------------------------------
// Orchestration: context, constants, session collection, Dream generation
// ---------------------------------------------------------------------------

const DREAM_RUNTIME_SESSION_ID: &str = "__dream__";
const DREAM_TRACING_TARGET: &str = "bamboo.auto_dream";
const DREAM_INTERVAL_SECS: u64 = 60 * 30;
const DREAM_FULL_REBUILD_INTERVAL_SECS: i64 = 60 * 60 * 24 * 30;
const DREAM_MAX_SESSIONS: usize = 12;
const DREAM_MAX_SUMMARY_CHARS: usize = 12_000;
const EXTRACTION_MAX_TOPICS_PER_SESSION: usize = 4;
const EXTRACTION_MAX_TOPIC_CHARS: usize = 1_500;
const EXTRACTION_MAX_CANDIDATES: usize = 8;

fn to_consolidation_sessions(
    entries: &[(SessionIndexEntry, Option<String>)],
) -> Vec<ConsolidationSessionInfo> {
    entries
        .iter()
        .map(|(entry, summary)| ConsolidationSessionInfo {
            id: entry.id.clone(),
            title: entry.title.clone(),
            kind: format!("{:?}", entry.kind),
            updated_at: entry.updated_at.to_rfc3339(),
            message_count: entry.message_count,
            last_run_status: entry.last_run_status.clone(),
            summary: summary.clone(),
        })
        .collect()
}

#[derive(Clone)]
pub struct AutoDreamContext {
    pub session_store: Arc<SessionStoreV2>,
    pub storage: Arc<dyn bamboo_agent_core::storage::Storage>,
    pub provider: Arc<dyn LLMProvider>,
    pub config: Arc<RwLock<Config>>,
}

fn memory_store_for_context(ctx: &AutoDreamContext) -> MemoryStore {
    MemoryStore::new(ctx.session_store.bamboo_home_dir())
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

#[derive(Debug, Clone)]
struct DreamSourceWindow {
    existing_dream: Option<String>,
    recent_durable_memory: Option<String>,
    durable_memory_index: Option<String>,
    sessions: Vec<(SessionIndexEntry, Option<String>)>,
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

async fn resolve_session_project_key(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    session_id: &str,
) -> Option<String> {
    ctx.storage
        .load_session(session_id)
        .await
        .ok()
        .flatten()
        .and_then(|session| session.metadata.get("workspace_path").cloned())
        .map(std::path::PathBuf::from)
        .map(|path| crate::memory_store::project_key_from_path(&path))
        .or_else(|| memory.project_key_for_session(Some(session_id)))
}

async fn collect_candidate_sessions_for_project(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_key: &str,
    since: DateTime<Utc>,
) -> Vec<(SessionIndexEntry, Option<String>)> {
    let mut out = Vec::new();
    for (entry, summary) in collect_candidate_sessions(ctx, since).await {
        if resolve_session_project_key(ctx, memory, &entry.id)
            .await
            .as_deref()
            != Some(project_key)
        {
            continue;
        }
        out.push((entry, summary));
        if out.len() >= DREAM_MAX_SESSIONS {
            break;
        }
    }
    out
}

async fn collect_candidate_session_contexts_from_sessions(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    sessions: Vec<(SessionIndexEntry, Option<String>)>,
) -> Vec<CandidateSessionContext> {
    let mut out = Vec::new();
    for (entry, summary) in sessions {
        let project_key = resolve_session_project_key(ctx, memory, &entry.id).await;
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
            project_key,
            entry,
            summary,
            topics,
        });
    }
    out
}

async fn collect_candidate_session_contexts(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    since: DateTime<Utc>,
) -> Vec<CandidateSessionContext> {
    collect_candidate_session_contexts_from_sessions(
        ctx,
        memory,
        collect_candidate_sessions(ctx, since).await,
    )
    .await
}

async fn collect_candidate_session_contexts_for_project(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_key: &str,
    since: DateTime<Utc>,
) -> Vec<CandidateSessionContext> {
    collect_candidate_session_contexts_from_sessions(
        ctx,
        memory,
        collect_candidate_sessions_for_project(ctx, memory, project_key, since).await,
    )
    .await
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

    let candidates_info: Vec<DreamCandidateInfo> = sessions
        .iter()
        .map(|session| DreamCandidateInfo {
            session_id: session.session_id.clone(),
            title: session.entry.title.clone(),
            project_key: session.project_key.clone(),
            updated_at: session.entry.updated_at.to_rfc3339(),
            summary: session.summary.clone(),
            topics: session.topics.clone(),
        })
        .collect();
    let prompt = build_extraction_prompt(&candidates_info);
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

async fn collect_stream_text(
    provider: Arc<dyn LLMProvider>,
    model: &str,
    prompt: String,
) -> Result<String, String> {
    let messages = vec![
        Message::system(
            "You are Bamboo's background Dream consolidator. Return only the Dream notebook body sections as plain markdown. Do not return an outer '# Bamboo Dream Notebook' title, metadata lines, or markdown fences."
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

async fn read_existing_dream_for_scope(
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
) -> Result<Option<String>, String> {
    match scope {
        MemoryScope::Global => memory
            .read_dream_view()
            .await
            .map_err(|error| format!("failed to read Dream notebook: {error}")),
        MemoryScope::Project => {
            let project_key = project_key
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "project Dream generation requires a project_key".to_string())?;
            memory
                .read_project_dream_view(project_key)
                .await
                .map_err(|error| {
                    format!("failed to read project Dream notebook for '{project_key}': {error}")
                })
        }
        MemoryScope::Session => Err("session-scoped Dream generation is not supported".to_string()),
    }
}

async fn read_recent_durable_memory_for_scope(
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
) -> Result<Option<String>, String> {
    memory
        .read_recent_view(scope, project_key)
        .await
        .map_err(|error| format!("failed to read recent durable memory view: {error}"))
}

async fn read_durable_memory_index_for_scope(
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
) -> Result<Option<String>, String> {
    memory
        .read_memory_view(scope, project_key)
        .await
        .map_err(|error| format!("failed to read durable memory index view: {error}"))
}

async fn write_dream_for_scope(
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
    content: &str,
) -> Result<std::path::PathBuf, String> {
    match scope {
        MemoryScope::Global => memory
            .write_dream_view(content)
            .await
            .map_err(|error| format!("failed to persist Dream notebook: {error}")),
        MemoryScope::Project => {
            let project_key = project_key
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "project Dream generation requires a project_key".to_string())?;
            memory
                .write_project_dream_view(project_key, content)
                .await
                .map_err(|error| {
                    format!("failed to persist project Dream notebook for '{project_key}': {error}")
                })
        }
        MemoryScope::Session => Err("session-scoped Dream generation is not supported".to_string()),
    }
}

async fn build_dream_notebook_body(
    ctx: &AutoDreamContext,
    model: &str,
    source_window: &DreamSourceWindow,
    generation_mode: DreamGenerationMode,
) -> Result<String, String> {
    match generation_mode {
        DreamGenerationMode::Refine => {
            tracing::info!(
                target: DREAM_TRACING_TARGET,
                event = "refine_attempt",
                model = model,
                session_count = source_window.sessions.len(),
                existing_dream_present = source_window.existing_dream.is_some(),
                recent_durable_memory_present = source_window.recent_durable_memory.is_some(),
                "Attempting refine-mode Dream synthesis"
            );

            let existing_dream_for_prompt = normalize_existing_dream_for_prompt(
                source_window.existing_dream.as_deref(),
                model,
                source_window.sessions.len(),
                DREAM_MAX_SUMMARY_CHARS,
            );
            let refine_prompt = build_refine_consolidation_prompt(
                existing_dream_for_prompt.as_deref(),
                source_window.recent_durable_memory.as_deref(),
                &to_consolidation_sessions(&source_window.sessions),
            );
            match collect_stream_text(ctx.provider.clone(), model, refine_prompt).await {
                Ok(raw_body) => {
                    match normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS) {
                        Ok(body) => {
                            tracing::info!(
                                target: DREAM_TRACING_TARGET,
                                event = "refine_success",
                                model = model,
                                session_count = source_window.sessions.len(),
                                notebook_body_chars = body.chars().count(),
                                existing_dream_present = source_window.existing_dream.is_some(),
                                recent_durable_memory_present = source_window.recent_durable_memory.is_some(),
                                "Refine-mode Dream synthesis succeeded"
                            );
                            Ok(body)
                        }
                        Err(error) => {
                            tracing::warn!(
                                target: DREAM_TRACING_TARGET,
                                event = "refine_output_normalization_failed",
                                model = model,
                                session_count = source_window.sessions.len(),
                                existing_dream_present = source_window.existing_dream.is_some(),
                                recent_durable_memory_present = source_window.recent_durable_memory.is_some(),
                                "[auto_dream] refine-mode Dream output normalization failed; falling back to incremental prompt: {}",
                                error
                            );
                            let prompt = build_consolidation_prompt(&to_consolidation_sessions(
                                &source_window.sessions,
                            ));
                            let raw_body =
                                collect_stream_text(ctx.provider.clone(), model, prompt).await?;
                            normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        target: DREAM_TRACING_TARGET,
                        event = "refine_provider_failed",
                        model = model,
                        session_count = source_window.sessions.len(),
                        existing_dream_present = source_window.existing_dream.is_some(),
                        recent_durable_memory_present = source_window.recent_durable_memory.is_some(),
                        "[auto_dream] refine-mode Dream synthesis failed; falling back to incremental prompt: {}",
                        error
                    );
                    let prompt = build_consolidation_prompt(&to_consolidation_sessions(
                        &source_window.sessions,
                    ));
                    let raw_body = collect_stream_text(ctx.provider.clone(), model, prompt).await?;
                    normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)
                }
            }
        }
        DreamGenerationMode::Rebuild => {
            tracing::info!(
                target: DREAM_TRACING_TARGET,
                event = "rebuild_attempt",
                model = model,
                session_count = source_window.sessions.len(),
                durable_memory_index_present = source_window.durable_memory_index.is_some(),
                "Attempting full rebuild Dream synthesis"
            );
            let prompt = build_rebuild_consolidation_prompt(
                source_window.durable_memory_index.as_deref(),
                &to_consolidation_sessions(&source_window.sessions),
            );
            let raw_body = collect_stream_text(ctx.provider.clone(), model, prompt).await?;
            normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)
        }
        DreamGenerationMode::Incremental => {
            let prompt =
                build_consolidation_prompt(&to_consolidation_sessions(&source_window.sessions));
            let raw_body = collect_stream_text(ctx.provider.clone(), model, prompt).await?;
            normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)
        }
    }
}

async fn run_auto_dream_once_for_scope(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
    require_auto_dream_enabled: bool,
) -> Result<Option<AutoDreamRunResult>, String> {
    let scope_label = match scope {
        MemoryScope::Global => "global",
        MemoryScope::Project => "project",
        MemoryScope::Session => "session",
    };

    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory.clone().unwrap_or_default();
    if require_auto_dream_enabled && !memory_cfg.auto_dream_enabled {
        tracing::info!(
            target: DREAM_TRACING_TARGET,
            event = "run_skip",
            reason = "auto_dream_disabled",
            scope = scope_label,
            project_key = project_key.unwrap_or(""),
            "Skipping Dream generation because auto_dream is disabled"
        );
        return Ok(None);
    }

    let Some(model) = config_snapshot.get_memory_background_model() else {
        tracing::warn!(
            target: DREAM_TRACING_TARGET,
            event = "run_skip",
            reason = "no_background_model",
            scope = scope_label,
            project_key = project_key.unwrap_or(""),
            "[auto_dream] skipped: no memory.background_model / provider.fast_model configured"
        );
        return Ok(None);
    };

    let now = Utc::now();
    let existing = read_existing_dream_for_scope(memory, scope, project_key).await?;
    let recent_durable_memory =
        read_recent_durable_memory_for_scope(memory, scope, project_key).await?;
    let durable_memory_index =
        read_durable_memory_index_for_scope(memory, scope, project_key).await?;
    let last_full_rebuild_at = existing.as_deref().and_then(parse_last_full_rebuild_at);
    let force_full_rebuild =
        should_force_full_rebuild(last_full_rebuild_at, now, DREAM_FULL_REBUILD_INTERVAL_SECS);
    let since = if force_full_rebuild {
        now - chrono::Duration::days(30)
    } else {
        match existing.as_deref().and_then(parse_last_consolidated_at) {
            Some(ts) => ts,
            None => now - chrono::Duration::hours(24),
        }
    };

    let sessions = match scope {
        MemoryScope::Global => collect_candidate_sessions(ctx, since).await,
        MemoryScope::Project => {
            let project_key = project_key
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "project Dream generation requires a project_key".to_string())?;
            collect_candidate_sessions_for_project(ctx, memory, project_key, since).await
        }
        MemoryScope::Session => {
            return Err("session-scoped Dream generation is not supported".to_string())
        }
    };
    if sessions.is_empty() {
        tracing::info!(
            target: DREAM_TRACING_TARGET,
            event = "run_skip",
            reason = "no_candidate_sessions",
            scope = scope_label,
            project_key = project_key.unwrap_or(""),
            model = model.as_str(),
            existing_dream_present = existing.is_some(),
            "Skipping Dream generation because there are no candidate sessions"
        );
        return Ok(None);
    }

    let generation_mode = if force_full_rebuild {
        DreamGenerationMode::Rebuild
    } else if should_use_dream_refine_mode(&memory_cfg) && existing.is_some() {
        DreamGenerationMode::Refine
    } else {
        DreamGenerationMode::Incremental
    };
    tracing::info!(
        target: DREAM_TRACING_TARGET,
        event = "run_start",
        scope = scope_label,
        project_key = project_key.unwrap_or(""),
        model = model.as_str(),
        session_count = sessions.len(),
        existing_dream_present = existing.is_some(),
        recent_durable_memory_present = recent_durable_memory.is_some(),
        durable_memory_index_present = durable_memory_index.is_some(),
        generation_mode = match generation_mode {
            DreamGenerationMode::Incremental => "incremental",
            DreamGenerationMode::Refine => "refine",
            DreamGenerationMode::Rebuild => "rebuild",
        },
        require_auto_dream_enabled = require_auto_dream_enabled,
        "Starting Dream generation run"
    );

    let source_window = DreamSourceWindow {
        existing_dream: existing,
        recent_durable_memory,
        durable_memory_index,
        sessions,
    };
    let notebook_body =
        build_dream_notebook_body(ctx, &model, &source_window, generation_mode).await?;
    let last_full_rebuild_line = if matches!(generation_mode, DreamGenerationMode::Rebuild) {
        format!("Last full rebuild at: {}\n", now.to_rfc3339())
    } else if let Some(existing_rebuild_at) = last_full_rebuild_at {
        format!(
            "Last full rebuild at: {}\n",
            existing_rebuild_at.to_rfc3339()
        )
    } else {
        String::new()
    };
    let final_note = match scope {
        MemoryScope::Global => format!(
            "# Bamboo Dream Notebook\n\nLast consolidated at: {}\n{}Sessions reviewed: {}\nModel: {}\n\n{}\n",
            now.to_rfc3339(),
            last_full_rebuild_line,
            source_window.sessions.len(),
            model,
            notebook_body.trim(),
        ),
        MemoryScope::Project => format!(
            "# Bamboo Dream Notebook\n\nProject key: {}\nLast consolidated at: {}\n{}Sessions reviewed: {}\nModel: {}\n\n{}\n",
            project_key.unwrap_or_default(),
            now.to_rfc3339(),
            last_full_rebuild_line,
            source_window.sessions.len(),
            model,
            notebook_body.trim(),
        ),
        MemoryScope::Session => unreachable!("session scope handled above"),
    };

    let note_path = write_dream_for_scope(memory, scope, project_key, &final_note).await?;

    let extraction_sessions = match scope {
        MemoryScope::Global => collect_candidate_session_contexts(ctx, memory, since).await,
        MemoryScope::Project => {
            let project_key = project_key
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "project Dream generation requires a project_key".to_string())?;
            collect_candidate_session_contexts_for_project(ctx, memory, project_key, since).await
        }
        MemoryScope::Session => unreachable!("session scope handled above"),
    };
    let extracted_count =
        extract_and_persist_durable_candidates(ctx, memory, &model, &extraction_sessions).await?;
    let notebook_chars = final_note.chars().count();

    tracing::info!(
        target: DREAM_TRACING_TARGET,
        event = "run_complete",
        scope = scope_label,
        project_key = project_key.unwrap_or(""),
        model = model.as_str(),
        session_count = source_window.sessions.len(),
        existing_dream_present = source_window.existing_dream.is_some(),
        recent_durable_memory_present = source_window.recent_durable_memory.is_some(),
        durable_memory_index_present = source_window.durable_memory_index.is_some(),
        generation_mode = match generation_mode {
            DreamGenerationMode::Incremental => "incremental",
            DreamGenerationMode::Refine => "refine",
            DreamGenerationMode::Rebuild => "rebuild",
        },
        notebook_chars = notebook_chars,
        durable_candidates_persisted = extracted_count,
        note_path = %note_path.display(),
        "Dream generation run completed"
    );

    Ok(Some(AutoDreamRunResult {
        used_model: model,
        session_count: source_window.sessions.len(),
        note_path,
        notebook_chars,
    }))
}

async fn run_auto_dream_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<AutoDreamRunResult>, String> {
    run_auto_dream_once_for_scope(ctx, memory, MemoryScope::Global, None, true).await
}

pub async fn run_auto_dream_once(
    ctx: &AutoDreamContext,
) -> Result<Option<AutoDreamRunResult>, String> {
    let memory = memory_store_for_context(ctx);
    run_auto_dream_once_with_store(ctx, &memory).await
}

pub async fn run_project_auto_dream_once(
    ctx: &AutoDreamContext,
    project_key: &str,
) -> Result<Option<AutoDreamRunResult>, String> {
    let memory = memory_store_for_context(ctx);
    run_project_auto_dream_once_with_store(ctx, &memory, project_key).await
}

async fn run_project_auto_dream_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    project_key: &str,
) -> Result<Option<AutoDreamRunResult>, String> {
    let project_key = project_key.trim();
    if project_key.is_empty() {
        return Err("project Dream generation requires a non-empty project_key".to_string());
    }
    run_auto_dream_once_for_scope(ctx, memory, MemoryScope::Project, Some(project_key), false).await
}

pub fn spawn_auto_dream_task(ctx: AutoDreamContext) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(DREAM_INTERVAL_SECS));
        loop {
            ticker.tick().await;
            if let Err(error) = run_auto_dream_once(&ctx).await {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "run_failed",
                    "[auto_dream] run failed: {}",
                    error
                );
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_chars_reports_truncation() {
        let result = truncate_chars("abcde", 3);
        assert_eq!(result, "abc...");
    }

    #[test]
    fn truncate_chars_keeps_short_text() {
        let result = truncate_chars("abc", 10);
        assert_eq!(result, "abc");
    }

    #[test]
    fn strip_json_fence_removes_fences() {
        assert_eq!(strip_json_fence("```json\n{}\n```"), "{}");
        assert_eq!(strip_json_fence("```\n{}\n```"), "{}");
        assert_eq!(strip_json_fence("{}"), "{}");
    }

    #[test]
    fn strip_markdown_fence_handles_variants() {
        assert_eq!(strip_markdown_fence("```markdown\nhi\n```"), "hi");
        assert_eq!(strip_markdown_fence("```md\nhi\n```"), "hi");
        assert_eq!(strip_markdown_fence("```\nhi\n```"), "hi");
        assert_eq!(strip_markdown_fence("hi"), "hi");
    }

    #[test]
    fn parse_extraction_candidates_accepts_fenced_json() {
        let input = "```json\n{\"candidates\":[{\"title\":\"T\",\"type\":\"user\",\"scope\":\"global\",\"content\":\"C\",\"tags\":[]}]}\n```";
        let candidates = parse_extraction_candidates(input).expect("should parse");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].title, "T");
    }

    #[test]
    fn parse_candidate_scope_defaults_to_project_when_key_available() {
        let candidate = DurableExtractionCandidate {
            title: "T".to_string(),
            kind: "user".to_string(),
            content: "C".to_string(),
            scope: None,
            tags: vec![],
            session_id: None,
            confidence: None,
        };
        assert_eq!(
            parse_candidate_scope(&candidate, Some("proj-1")),
            crate::memory_store::MemoryScope::Project
        );
    }

    #[test]
    fn parse_candidate_type_maps_known_types() {
        assert!(parse_candidate_type("user").is_some());
        assert!(parse_candidate_type("feedback").is_some());
        assert!(parse_candidate_type("project").is_some());
        assert!(parse_candidate_type("reference").is_some());
        assert!(parse_candidate_type("unknown").is_none());
    }

    #[test]
    fn strip_dream_notebook_wrapper_extracts_body() {
        let input = "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-01-01T00:00:00Z\nSessions reviewed: 1\nModel: test\n\n## Body\ncontent";
        let body = strip_dream_notebook_wrapper(input).expect("should extract");
        assert!(body.contains("## Body"));
        assert!(!body.contains("Bamboo Dream Notebook"));
        assert!(!body.contains("Last consolidated"));
    }

    #[test]
    fn normalize_dream_notebook_body_strips_wrapper() {
        let input = "# Bamboo Dream Notebook\n\nModel: test\n\n## Section\ndata\n";
        let result = normalize_dream_notebook_body(input, 10000).expect("should normalize");
        assert!(result.contains("## Section"));
        assert!(!result.contains("Bamboo Dream Notebook"));
    }

    #[test]
    fn normalize_dream_notebook_body_rejects_empty() {
        assert!(normalize_dream_notebook_body("", 10000).is_err());
    }

    #[test]
    fn build_extraction_prompt_includes_candidates() {
        let candidates = vec![DreamCandidateInfo {
            session_id: "s-1".to_string(),
            title: "Title 1".to_string(),
            project_key: Some("proj-a".to_string()),
            updated_at: "2026-04-01T00:00:00Z".to_string(),
            summary: Some("Important summary".to_string()),
            topics: vec![("topic-a".to_string(), "content-a".to_string())],
        }];
        let prompt = build_extraction_prompt(&candidates);
        assert!(prompt.contains("Bamboo Durable Memory Extraction"));
        assert!(prompt.contains("s-1"));
        assert!(prompt.contains("Title 1"));
        assert!(prompt.contains("proj-a"));
        assert!(prompt.contains("Important summary"));
        assert!(prompt.contains("topic-a"));
    }

    #[test]
    fn build_extraction_prompt_handles_empty_candidates() {
        let prompt = build_extraction_prompt(&[]);
        assert!(prompt.contains("Bamboo Durable Memory Extraction"));
        assert!(prompt.contains("Candidate sessions"));
    }

    fn sample_consolidation_session(id: &str) -> ConsolidationSessionInfo {
        ConsolidationSessionInfo {
            id: id.to_string(),
            title: format!("Title for {id}"),
            kind: "Root".to_string(),
            updated_at: "2026-04-01T00:00:00Z".to_string(),
            message_count: 10,
            last_run_status: Some("completed".to_string()),
            summary: Some("Important summary".to_string()),
        }
    }

    #[test]
    fn consolidation_prompt_includes_session_metadata_and_summary() {
        let prompt = build_consolidation_prompt(&[sample_consolidation_session("session-1")]);
        assert!(prompt.contains("Bamboo Dream Consolidation"));
        assert!(prompt.contains("session-1"));
        assert!(prompt.contains("Important summary"));
        assert!(prompt.contains("## Current durable context"));
    }

    #[test]
    fn refine_consolidation_prompt_includes_existing_dream() {
        let prompt = build_refine_consolidation_prompt(
            Some("## Current durable context\n- Existing durable thread"),
            Some("# Recent Memory Updates\n\n- `mem-1` User prefers concise plans"),
            &[sample_consolidation_session("session-2")],
        );
        assert!(prompt.contains("## Existing Dream notebook"));
        assert!(prompt.contains("Existing durable thread"));
        assert!(prompt.contains("## Recent durable memory updates"));
        assert!(prompt.contains("User prefers concise plans"));
        assert!(prompt.contains("start from it and preserve still-valid durable context"));
        assert!(prompt.contains("session-2"));
    }

    #[test]
    fn rebuild_consolidation_prompt_includes_durable_memory_index() {
        let prompt = build_rebuild_consolidation_prompt(
            Some("# Bamboo Memory Index\n\n- `mem-1` Release freeze decision"),
            &[sample_consolidation_session("session-3")],
        );
        assert!(prompt.contains("## Durable memory index"));
        assert!(prompt.contains("Release freeze decision"));
        assert!(prompt.contains("canonical durable memory plus recent session activity"));
        assert!(prompt.contains("session-3"));
    }

    // -----------------------------------------------------------------------
    // Orchestration tests
    // -----------------------------------------------------------------------

    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream;

    use bamboo_agent_core::storage::Storage;
    use bamboo_infrastructure::{LLMError, LLMStream};

    #[derive(Debug, Clone)]
    enum SequenceStep {
        Response(String),
        Fail(String),
    }

    #[derive(Clone)]
    struct SequenceProvider {
        steps: Arc<Mutex<Vec<SequenceStep>>>,
        prompts: Arc<Mutex<Vec<String>>>,
    }

    impl SequenceProvider {
        fn new(responses: Vec<String>) -> Self {
            Self::from_steps(responses.into_iter().map(SequenceStep::Response).collect())
        }

        fn from_steps(steps: Vec<SequenceStep>) -> Self {
            Self {
                steps: Arc::new(Mutex::new(steps)),
                prompts: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn recorded_prompts(&self) -> Vec<String> {
            self.prompts.lock().expect("lock poisoned").clone()
        }
    }

    #[async_trait]
    impl LLMProvider for SequenceProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            if let Some(prompt) = messages.last().map(|message| message.content.clone()) {
                self.prompts.lock().expect("lock poisoned").push(prompt);
            }
            let next = self.steps.lock().expect("lock poisoned").remove(0);
            match next {
                SequenceStep::Response(text) => Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::Token(text)),
                    Ok(LLMChunk::Done),
                ]))),
                SequenceStep::Fail(error) => Err(LLMError::Stream(error)),
            }
        }
    }

    #[test]
    fn parse_last_consolidated_at_reads_frontmatter_line() {
        let note = "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 3\n";
        let parsed = parse_last_consolidated_at(note).expect("timestamp should parse");
        assert_eq!(parsed.to_rfc3339(), "2026-04-02T16:00:00+00:00");
    }

    #[tokio::test]
    async fn extract_and_persist_durable_candidates_writes_memory_and_marks_session() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

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
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let mut session = bamboo_agent_core::Session::new("session-auto", "model");
        session.title = "Auto memory test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            temp_dir
                .path()
                .join("workspace-a")
                .to_string_lossy()
                .to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
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

        let project_key =
            crate::memory_store::project_key_from_path(&temp_dir.path().join("workspace-a"));
        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key),
                Some("terse recap"),
                None,
                None,
                &crate::memory_store::MemoryQueryOptions {
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
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let mut session = bamboo_agent_core::Session::new("session-empty", "model");
        session.metadata.insert(
            "workspace_path".to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );
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
        let writes =
            extract_and_persist_durable_candidates(&context, &memory, "fast-model", &sessions)
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
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

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
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let mut session = bamboo_agent_core::Session::new("session-dream-run", "model");
        session.title = "Dream run test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            temp_dir
                .path()
                .join("workspace-run")
                .to_string_lossy()
                .to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
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

        let project_key =
            crate::memory_store::project_key_from_path(&temp_dir.path().join("workspace-run"));
        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key),
                Some("concise answers"),
                None,
                None,
                &crate::memory_store::MemoryQueryOptions {
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
    async fn run_project_auto_dream_once_filters_sessions_by_project_and_writes_project_dream() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace_a = temp_dir.path().join("workspace-a");
        let workspace_b = temp_dir.path().join("workspace-b");
        std::fs::create_dir_all(&workspace_a).expect("workspace a");
        std::fs::create_dir_all(&workspace_b).expect("workspace b");
        let project_key_a = crate::memory_store::project_key_from_path(&workspace_a);

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "## Current durable context\n- Project A signal only\n\n## Cross-session patterns\n- Focus on project A\n\n## Active threads to remember\n- Ship project A\n\n## Stable constraints and preferences\n- Keep scope isolated\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[{\"title\":\"Project A prefers concise planning\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"Project A plans should stay concise and scoped.\",\"tags\":[\"planning\"],\"session_id\":\"session-project-a\"}]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let mut session_a = bamboo_agent_core::Session::new("session-project-a", "model");
        session_a.title = "Project A session".to_string();
        session_a.metadata.insert(
            "workspace_path".to_string(),
            workspace_a.to_string_lossy().to_string(),
        );
        session_a.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Project A stable direction.",
            4,
            160,
        ));
        session_a.add_message(Message::user("Keep project A plans concise."));
        storage
            .save_session(&session_a)
            .await
            .expect("save session a");

        let mut session_b = bamboo_agent_core::Session::new("session-project-b", "model");
        session_b.title = "Project B session".to_string();
        session_b.metadata.insert(
            "workspace_path".to_string(),
            workspace_b.to_string_lossy().to_string(),
        );
        session_b.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Project B unrelated direction.",
            4,
            160,
        ));
        session_b.add_message(Message::user("This is unrelated project B context."));
        storage
            .save_session(&session_b)
            .await
            .expect("save session b");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic(
                "session-project-a",
                "default",
                "Project A planning should remain concise.",
            )
            .await
            .expect("write session topic a");
        memory
            .write_session_topic(
                "session-project-b",
                "default",
                "Project B note that should not be included.",
            )
            .await
            .expect("write session topic b");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let result = run_project_auto_dream_once_with_store(&context, &memory, &project_key_a)
            .await
            .expect("project auto dream should succeed")
            .expect("project auto dream should produce output");
        assert_eq!(result.used_model, "fast-model");
        assert_eq!(result.session_count, 1);

        let project_dream = memory
            .read_project_dream_view(&project_key_a)
            .await
            .expect("read project dream")
            .expect("project dream should exist");
        assert!(project_dream.contains("Bamboo Dream Notebook"));
        assert!(project_dream.contains("Project key: "));
        assert!(project_dream.contains(&project_key_a));
        assert!(project_dream.contains("Project A signal only"));
        assert!(!project_dream.contains("unrelated project B"));

        let global_dream = memory.read_dream_view().await.expect("read global dream");
        assert!(global_dream.is_none());

        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key_a),
                Some("concise planning"),
                None,
                None,
                &crate::memory_store::MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(2000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .expect("query should succeed");
        assert_eq!(results.matched_count, 1);
        assert_eq!(results.items[0].title, "Project A prefers concise planning");
    }

    #[tokio::test]
    async fn run_project_auto_dream_once_returns_none_without_target_project_sessions_and_preserves_existing_dream(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace_other = temp_dir.path().join("workspace-other");
        let workspace_target = temp_dir.path().join("workspace-target");
        std::fs::create_dir_all(&workspace_other).expect("workspace other");
        std::fs::create_dir_all(&workspace_target).expect("workspace target");
        let target_project_key = crate::memory_store::project_key_from_path(&workspace_target);

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let mut other_session = bamboo_agent_core::Session::new("session-other-project", "model");
        other_session.title = "Other project session".to_string();
        other_session.metadata.insert(
            "workspace_path".to_string(),
            workspace_other.to_string_lossy().to_string(),
        );
        other_session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Other project only.",
            2,
            80,
        ));
        other_session.add_message(Message::user("Other project context only."));
        storage
            .save_session(&other_session)
            .await
            .expect("save other session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_project_dream_view(
                &target_project_key,
                "# Bamboo Dream Notebook\n\nExisting target project dream",
            )
            .await
            .expect("write existing project dream");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let result = run_project_auto_dream_once_with_store(&context, &memory, &target_project_key)
            .await
            .expect("project auto dream without sessions should not error");
        assert!(result.is_none());

        let project_dream = memory
            .read_project_dream_view(&target_project_key)
            .await
            .expect("read project dream")
            .expect("existing dream should remain");
        assert!(project_dream.contains("Existing target project dream"));
    }

    #[tokio::test]
    async fn run_project_auto_dream_once_still_runs_when_auto_background_dream_is_disabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace = temp_dir.path().join("workspace-manual-project-dream");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_key = crate::memory_store::project_key_from_path(&workspace);

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "## Current durable context\n- Manual project dream worked\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let mut session = bamboo_agent_core::Session::new("session-manual-project-dream", "model");
        session.title = "Manual project dream session".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Manual project dream summary.",
            3,
            100,
        ));
        session.add_message(Message::user("Generate a project-scoped dream manually."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic(
                "session-manual-project-dream",
                "default",
                "Manual project dream note.",
            )
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider,
            config,
        };
        let result = run_project_auto_dream_once_with_store(&context, &memory, &project_key)
            .await
            .expect(
                "manual project dream should succeed even when auto background dream is disabled",
            )
            .expect("manual project dream should produce output");
        assert_eq!(result.session_count, 1);

        let project_dream = memory
            .read_project_dream_view(&project_key)
            .await
            .expect("read project dream")
            .expect("project dream should exist");
        assert!(project_dream.contains("Manual project dream worked"));
    }

    #[tokio::test]
    async fn run_auto_dream_once_refine_mode_includes_existing_dream_in_prompt() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "## Current durable context\n- Refined durable theme\n\n## Cross-session patterns\n- Keep continuity\n\n## Active threads to remember\n- Update the notebook\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[]}".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                dream_refine_mode: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let workspace = temp_dir.path().join("workspace-refine-mode");
        std::fs::create_dir_all(&workspace).expect("workspace dir");

        let mut session = bamboo_agent_core::Session::new("session-refine-mode", "model");
        session.title = "Refine mode test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Recent session summary for refine mode.",
            3,
            120,
        ));
        session.add_message(Message::user("Update the dream with the latest thread."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_dream_view(
                "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing durable thread\n",
            )
            .await
            .expect("write existing dream");
        memory
            .write_session_topic("session-refine-mode", "default", "Recent session note.")
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider: provider_handle,
            config,
        };

        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("refine-mode auto dream should succeed")
            .expect("dream output should be produced");
        assert_eq!(result.session_count, 1);

        let prompts = provider.recorded_prompts();
        assert!(prompts.len() >= 2);
        assert!(prompts[0].contains("## Existing Dream notebook"));
        assert!(prompts[0].contains("Existing durable thread"));
        assert!(prompts[0].contains("## Recent durable memory updates"));
        assert!(prompts[0].contains("start from it and preserve still-valid durable context"));
    }

    #[tokio::test]
    async fn run_auto_dream_once_refine_mode_includes_recent_durable_memory_in_prompt() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "## Current durable context\n- Refined from durable memory\n\n## Cross-session patterns\n- Keep continuity\n\n## Active threads to remember\n- Update the notebook\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[]}".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                dream_refine_mode: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let workspace = temp_dir.path().join("workspace-refine-recent-memory");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_key = crate::memory_store::project_key_from_path(&workspace);

        let mut session = bamboo_agent_core::Session::new("session-refine-recent-memory", "model");
        session.title = "Refine recent memory test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Recent session summary for refine recent memory.",
            3,
            120,
        ));
        session.add_message(Message::user(
            "Update the dream with recent durable memory.",
        ));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_project_dream_view(
                &project_key,
                "# Bamboo Dream Notebook\n\nProject key: project\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing durable thread\n",
            )
            .await
            .expect("write existing project dream");
        memory
            .write_memory(
                MemoryScope::Project,
                Some(&project_key),
                crate::memory_store::DurableMemoryType::Project,
                "Release freeze rule",
                "The release freeze starts on Tuesday for mobile.",
                &["release".to_string(), "freeze".to_string()],
                Some("session-refine-recent-memory"),
                "main-model",
                false,
            )
            .await
            .expect("write recent durable memory");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider: provider_handle,
            config,
        };

        let _ = run_project_auto_dream_once_with_store(&context, &memory, &project_key)
            .await
            .expect("refine recent-memory auto dream should succeed")
            .expect("dream output should be produced");

        let prompts = provider.recorded_prompts();
        assert!(prompts.len() >= 2);
        assert!(prompts[0].contains("## Recent durable memory updates"));
        assert!(prompts[0].contains("Release freeze rule"));
        assert!(prompts[0].contains("Recent Memory Updates"));
    }

    #[tokio::test]
    async fn run_auto_dream_once_forces_periodic_full_rebuild_using_memory_index() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "## Current durable context\n- Rebuilt from durable memory index\n\n## Cross-session patterns\n- Canonical project history\n\n## Active threads to remember\n- Refresh active blockers\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            "{\"candidates\":[]}".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                dream_refine_mode: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let workspace = temp_dir.path().join("workspace-rebuild-mode");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_key = crate::memory_store::project_key_from_path(&workspace);

        let mut session = bamboo_agent_core::Session::new("session-rebuild-mode", "model");
        session.title = "Rebuild mode test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Recent session summary for rebuild mode.",
            3,
            120,
        ));
        session.add_message(Message::user(
            "Refresh the project dream from canonical memory.",
        ));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_project_dream_view(
                &project_key,
                "# Bamboo Dream Notebook\n\nProject key: project\nLast consolidated at: 2026-02-02T16:00:00Z\nLast full rebuild at: 2026-02-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing project dream\n",
            )
            .await
            .expect("write existing project dream");
        memory
            .write_memory(
                MemoryScope::Project,
                Some(&project_key),
                crate::memory_store::DurableMemoryType::Project,
                "Canonical release decision",
                "Release freeze starts Tuesday and all mobile changes require review.",
                &["release".to_string(), "mobile".to_string()],
                Some("session-rebuild-mode"),
                "main-model",
                false,
            )
            .await
            .expect("write project durable memory");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider: provider_handle,
            config,
        };

        let result = run_project_auto_dream_once_with_store(&context, &memory, &project_key)
            .await
            .expect("rebuild auto dream should succeed")
            .expect("rebuild dream output should be produced");
        assert_eq!(result.session_count, 1);

        let prompts = provider.recorded_prompts();
        assert!(prompts.len() >= 2);
        assert!(prompts[0].contains("## Durable memory index"));
        assert!(prompts[0].contains("Canonical release decision"));
        assert!(prompts[0].contains("canonical durable memory plus recent session activity"));

        let dream = memory
            .read_project_dream_view(&project_key)
            .await
            .expect("read project dream")
            .expect("project dream should exist");
        assert!(dream.contains("Rebuilt from durable memory index"));
        assert!(dream.contains("Last full rebuild at:"));
    }

    #[test]
    fn normalize_dream_notebook_body_strips_nested_fenced_notebook_wrapper() {
        let raw = r#"
```md
# Bamboo Dream Notebook

Last consolidated at: 2026-04-10T06:28:54.680302+00:00
Sessions reviewed: 2
Model: gpt-5-mini

## Current durable context
- Existing durable thread

## Cross-session patterns
- Keep continuity

## Active threads to remember
- Update the notebook

## Stable constraints and preferences
- None

## Open risks or questions
- None
```
"#;

        let normalized = normalize_dream_notebook_body(raw, DREAM_MAX_SUMMARY_CHARS)
            .expect("normalization should succeed");
        assert!(!normalized.contains("```md"));
        assert!(!normalized.contains("# Bamboo Dream Notebook"));
        assert!(normalized.contains("## Current durable context"));
        assert!(normalized.contains("Existing durable thread"));
    }

    #[tokio::test]
    async fn run_auto_dream_once_refine_mode_normalizes_nested_notebook_output() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "```md\n# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-10T06:28:54.680302+00:00\nSessions reviewed: 2\nModel: gpt-5-mini\n\n## Current durable context\n- Refined durable theme\n\n## Cross-session patterns\n- Keep continuity\n\n## Active threads to remember\n- Update the notebook\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None\n```".to_string(),
            "{\"candidates\":[]}".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                dream_refine_mode: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let workspace = temp_dir.path().join("workspace-refine-normalize");
        std::fs::create_dir_all(&workspace).expect("workspace dir");

        let mut session = bamboo_agent_core::Session::new("session-refine-normalize", "model");
        session.title = "Refine normalize test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Recent session summary for refine normalization.",
            3,
            120,
        ));
        session.add_message(Message::user("Normalize the refined dream output."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_dream_view(
                "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing durable thread\n",
            )
            .await
            .expect("write existing dream");
        memory
            .write_session_topic(
                "session-refine-normalize",
                "default",
                "Recent session note.",
            )
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider: provider_handle,
            config,
        };

        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("refine normalize auto dream should succeed")
            .expect("dream output should be produced");
        assert_eq!(result.session_count, 1);

        let dream = memory
            .read_dream_view()
            .await
            .expect("read dream view")
            .expect("dream should exist");
        assert!(dream.contains("Refined durable theme"));
        assert!(!dream.contains("```md"));
        assert_eq!(dream.matches("# Bamboo Dream Notebook").count(), 1);
    }

    #[tokio::test]
    async fn run_auto_dream_once_refine_mode_falls_back_to_legacy_prompt_on_failure() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::from_steps(vec![
            SequenceStep::Fail("refine prompt failed".to_string()),
            SequenceStep::Response(
                "## Current durable context\n- Legacy fallback result\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            ),
            SequenceStep::Response("{\"candidates\":[]}".to_string()),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                dream_refine_mode: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
            }),
            ..Config::default()
        }));

        let workspace = temp_dir.path().join("workspace-refine-fallback");
        std::fs::create_dir_all(&workspace).expect("workspace dir");

        let mut session = bamboo_agent_core::Session::new("session-refine-fallback", "model");
        session.title = "Refine fallback test".to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Recent summary for fallback mode.",
            3,
            120,
        ));
        session.add_message(Message::user("Please update the dream safely."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_dream_view(
                "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing durable thread\n",
            )
            .await
            .expect("write existing dream");
        memory
            .write_session_topic("session-refine-fallback", "default", "Recent session note.")
            .await
            .expect("write session topic");

        let context = AutoDreamContext {
            session_store,
            storage,
            provider: provider_handle,
            config,
        };

        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("fallback auto dream should succeed")
            .expect("dream output should be produced");
        assert_eq!(result.session_count, 1);

        let prompts = provider.recorded_prompts();
        assert!(prompts.len() >= 3);
        assert!(prompts[0].contains("## Existing Dream notebook"));
        assert!(!prompts[1].contains("## Existing Dream notebook"));
        assert!(prompts[1].contains("## Recent sessions"));

        let dream = memory
            .read_dream_view()
            .await
            .expect("read dream view")
            .expect("dream should exist after fallback");
        assert!(dream.contains("Legacy fallback result"));
    }

    #[tokio::test]
    async fn run_auto_dream_once_returns_none_when_disabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                ..bamboo_infrastructure::config::MemoryConfig::default()
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
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(Config {
            memory: Some(bamboo_infrastructure::config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_infrastructure::config::MemoryConfig::default()
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
