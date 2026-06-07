//! Auto-dream pure helpers: extraction/consolidation types, response parsing,
//! prompt construction, and Dream-notebook normalization.
//!
//! These are infrastructure-free building blocks consumed by the live Dream
//! orchestration in `bamboo_engine::auto_dream`. The orchestration itself
//! (LLM provider / session-store driven runs) lives in the engine, not here.

use serde::Deserialize;

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

#[derive(serde::Deserialize)]
struct SplitEnvelope {
    #[serde(default)]
    pieces: Vec<SplitPieceRaw>,
}

#[derive(serde::Deserialize)]
struct SplitPieceRaw {
    #[serde(default)]
    title: String,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    content: String,
    #[serde(default)]
    tags: Vec<String>,
}

/// Parse the background model's blob-split JSON into atomic split pieces.
/// Pure; skips pieces missing a title or content.
pub fn parse_split_pieces(raw: &str) -> Result<Vec<crate::memory_store::MemorySplitPiece>, String> {
    let payload = strip_json_fence(raw);
    let parsed: SplitEnvelope = serde_json::from_str(payload)
        .map_err(|error| format!("failed to parse split pieces: {error}"))?;
    let pieces = parsed
        .pieces
        .into_iter()
        .filter_map(|piece| {
            let title = piece.title.trim().to_string();
            let content = piece.content.trim().to_string();
            if title.is_empty() || content.is_empty() {
                return None;
            }
            Some(crate::memory_store::MemorySplitPiece {
                title,
                r#type: piece.kind.as_deref().and_then(parse_candidate_type),
                content,
                tags: piece.tags,
            })
        })
        .collect();
    Ok(pieces)
}

/// Build the prompt asking the background model to split a multi-topic "blob"
/// memory into atomic pieces. Pure — formats text only.
pub fn build_blob_split_prompt(title: &str, body: &str) -> String {
    let mut prompt = String::from("# Bamboo Memory Split\n\n");
    prompt.push_str(
        "The durable memory below has accreted multiple facts and must be split into atomic memories.\n\n",
    );
    prompt.push_str("Rules:\n");
    prompt.push_str("- Return JSON only: {\"pieces\":[{\"title\":string,\"type\":\"user\"|\"feedback\"|\"project\"|\"reference\",\"content\":string,\"tags\":string[]}]}\n");
    prompt.push_str("- Each piece must capture exactly ONE atomic fact/decision/preference. Never combine unrelated facts.\n");
    prompt.push_str("- The title must concisely summarize that piece's own content so it is findable by keyword search.\n");
    prompt.push_str("- Preserve the original wording of each fact; do not invent facts. Drop only exact duplicates.\n");
    prompt.push_str(
        "- If the memory is actually a single coherent fact, return exactly one piece.\n\n",
    );
    prompt.push_str("## Memory\n");
    prompt.push_str(&format!("- title: {title}\n"));
    prompt.push_str("- body:\n```md\n");
    prompt.push_str(body);
    prompt.push_str("\n```\n");
    prompt
}

#[derive(serde::Deserialize)]
struct DedupDecisionRaw {
    #[serde(default)]
    same_fact: bool,
    #[serde(default)]
    merged: Option<SplitPieceRaw>,
}

/// Parse the background model's dedup decision. Returns `Some(piece)` ONLY when the
/// model judged the cluster to be the same fact AND returned a usable merged memory;
/// `None` means "leave them separate" (distinct facts, or unusable output). Pure.
pub fn parse_dedup_decision(
    raw: &str,
) -> Result<Option<crate::memory_store::MemorySplitPiece>, String> {
    let payload = strip_json_fence(raw);
    let parsed: DedupDecisionRaw = serde_json::from_str(payload)
        .map_err(|error| format!("failed to parse dedup decision: {error}"))?;
    if !parsed.same_fact {
        return Ok(None);
    }
    let Some(piece) = parsed.merged else {
        return Ok(None);
    };
    let title = piece.title.trim().to_string();
    let content = piece.content.trim().to_string();
    if title.is_empty() || content.is_empty() {
        return Ok(None);
    }
    Ok(Some(crate::memory_store::MemorySplitPiece {
        title,
        r#type: piece.kind.as_deref().and_then(parse_candidate_type),
        content,
        tags: piece.tags,
    }))
}

/// Build the prompt asking the background model to judge whether a cluster of
/// near-duplicate memories is the SAME fact and, if so, consolidate them into one
/// atomic memory. Pure — formats text only.
pub fn build_dedup_prompt(members: &[(String, String)]) -> String {
    let mut prompt = String::from("# Bamboo Memory Deduplication\n\n");
    prompt.push_str(
        "The durable memories below were flagged as possible duplicates of each other.\n\n",
    );
    prompt
        .push_str("Decide whether they all describe the SAME single fact/decision/preference.\n\n");
    prompt.push_str("Rules:\n");
    prompt.push_str("- Return JSON only: {\"same_fact\":boolean,\"merged\":{\"title\":string,\"type\":\"user\"|\"feedback\"|\"project\"|\"reference\",\"content\":string,\"tags\":string[]}}\n");
    prompt.push_str("- Set same_fact=true and provide `merged` ONLY if they are genuinely the same fact. Merge them into ONE atomic memory that preserves every distinct detail, with a specific, keyword-findable title.\n");
    prompt.push_str("- Set same_fact=false (omit `merged`) if they are merely related but distinct facts. When unsure, prefer false — never merge facts that are not the same.\n");
    prompt.push_str(
        "- Preserve original wording; do not invent facts. Drop only exact redundancy.\n\n",
    );
    prompt.push_str("## Memories\n");
    for (index, (title, body)) in members.iter().enumerate() {
        prompt.push_str(&format!("\n### Memory {}\n", index + 1));
        prompt.push_str(&format!("- title: {title}\n"));
        prompt.push_str("- body:\n```md\n");
        prompt.push_str(body);
        prompt.push_str("\n```\n");
    }
    prompt
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
    prompt.push_str("- Each candidate must capture exactly ONE atomic fact/decision/preference. Never combine unrelated facts into a single candidate.\n");
    prompt.push_str("- The title must concisely summarize THAT candidate's own content so it can be found later by keyword search; never use a generic title that does not match the content.\n");
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
    prompt.push_str("- Only fold a fact into an existing memory when it is the same fact. Keep unrelated facts as separate memories; never join unrelated facts with separators.\n");
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
    memory_cfg: &bamboo_config::MemoryConfig,
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
    use bamboo_llm::{LLMError, LLMStream};

}
