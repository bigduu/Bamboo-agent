use std::path::Path;

use crate::agent::core::memory_store::{
    project_key_from_path, render_memory_freshness_note, shortlist_relevant_memories,
    truncate_chars as memory_truncate_chars, FreshnessKind, MemoryRecallCandidate,
    MemoryRecallOptions, MemoryScope, MemoryStore,
};
use crate::agent::core::Session;
use crate::agent::tools::tools::workspace_state;

use super::system_sections::strip_existing_prompt_block;

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
/// Max chars per-topic shown in the system prompt.
const SESSION_NOTE_PROMPT_MAX_CHARS_PER_TOPIC: usize = 4_000;
/// Max total chars for all rendered session-note topics combined.
const SESSION_NOTE_PROMPT_MAX_TOTAL_CHARS: usize = 6_000;
/// Max chars injected from the current project's durable MEMORY index.
const PROJECT_MEMORY_INDEX_PROMPT_MAX_CHARS: usize = 1_800;
/// Top-k recall items rendered into the prompt.
const RELEVANT_MEMORY_RESULT_LIMIT: usize = 3;
/// Max chars used by the full relevant-memory section.
const RELEVANT_MEMORY_TOTAL_MAX_CHARS: usize = 1_600;
/// Max chars used by each relevant-memory summary snippet.
const RELEVANT_MEMORY_PER_ITEM_MAX_CHARS: usize = 220;
/// Max chars injected from the global Dream notebook fallback.
const GLOBAL_DREAM_NOTEBOOK_PROMPT_MAX_CHARS: usize = 1_500;
const EXTERNAL_MEMORY_TOOL_NAME: &str = "session_note";

/// Context usage percentage at which we warn the LLM to save memory.
/// This should be below the budget system's compression trigger (~90%).
const CONTEXT_PRESSURE_WARNING_THRESHOLD: f64 = 70.0;

#[derive(Debug, Clone)]
struct TopicSnippet {
    name: String,
    content: String,
    truncated: bool,
    full_len: usize,
}

#[derive(Debug, Clone)]
struct LoadedSnippet {
    content: String,
    truncated: bool,
    full_len: usize,
}

#[derive(Debug, Clone)]
struct ProjectMemoryIndexSnippet {
    project_key: String,
    content: String,
    truncated: bool,
    full_len: usize,
    freshness_note: Option<String>,
}

#[derive(Debug, Clone)]
struct ProjectDreamSnippet {
    project_key: String,
    content: String,
    truncated: bool,
    full_len: usize,
}

#[derive(Debug, Clone)]
struct RelevantMemorySnippet {
    title: String,
    scope: MemoryScope,
    status: String,
    score: f64,
    summary: String,
    freshness_note: Option<String>,
}

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

fn count_chars(value: &str) -> usize {
    value.chars().count()
}

pub(super) async fn inject_external_memory_into_system_message(session: &mut Session) {
    let memory = MemoryStore::with_defaults();
    inject_external_memory_into_system_message_with_store(session, &memory).await;
}

pub(super) async fn inject_external_memory_into_system_message_with_store(
    session: &mut Session,
    memory: &MemoryStore,
) {
    let Some(system_message_index) = session
        .messages
        .iter()
        .position(|message| matches!(message.role, crate::agent::core::Role::System))
    else {
        return;
    };

    // Remove any previously injected memory block, then re-append a fresh
    // memory section for this round.
    let base_prompt =
        strip_existing_external_memory(&session.messages[system_message_index].content);

    let session_id = session.id.as_str();
    let resolved_project_key = resolve_prompt_project_key(session);
    let session_note_snippets = load_session_note_snippets(memory, session_id).await;
    let project_memory_index =
        load_project_memory_index_snippet(memory, session_id, resolved_project_key.as_deref())
            .await;
    let relevant_memory_snippets =
        load_relevant_memory_snippets(session, memory, session_id, resolved_project_key.as_deref())
            .await;
    let project_dream =
        load_project_dream_snippet(memory, session_id, resolved_project_key.as_deref()).await;
    let global_dream_fallback = if project_dream.is_none() && project_memory_index.is_none() {
        load_global_dream_fallback_snippet(memory, session_id).await
    } else {
        None
    };

    let section = build_external_memory_section(
        session,
        &session_note_snippets,
        project_memory_index.as_ref(),
        &relevant_memory_snippets,
        project_dream.as_ref(),
        global_dream_fallback.as_ref(),
    );

    session.messages[system_message_index].content =
        format!("{}{}", base_prompt.trim_end(), section);

    tracing::info!(
        "[{}] External memory injected: project_key_resolved={}, project_index_loaded={}, project_index_chars={}, relevant_query_present={}, relevant_count={}, project_dream_loaded={}, project_dream_chars={}, global_dream_fallback_used={}, global_dream_chars={}, session_topics={}, session_note_chars={}, truncated_topics={}",
        session_id,
        resolved_project_key.as_deref().unwrap_or(""),
        project_memory_index.is_some(),
        project_memory_index
            .as_ref()
            .map(|snippet| count_chars(&snippet.content))
            .unwrap_or(0),
        latest_user_query_text(session).is_some(),
        relevant_memory_snippets.len(),
        project_dream.is_some(),
        project_dream
            .as_ref()
            .map(|snippet| count_chars(&snippet.content))
            .unwrap_or(0),
        global_dream_fallback.is_some(),
        global_dream_fallback
            .as_ref()
            .map(|snippet| count_chars(&snippet.content))
            .unwrap_or(0),
        session_note_snippets.len(),
        session_note_snippets
            .iter()
            .map(|snippet| count_chars(&snippet.content))
            .sum::<usize>(),
        session_note_snippets
            .iter()
            .filter(|snippet| snippet.truncated)
            .count(),
    );
}

fn resolve_prompt_project_key(session: &Session) -> Option<String> {
    session
        .metadata
        .get("workspace_path")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Path::new)
        .map(project_key_from_path)
        .or_else(|| {
            workspace_state::get_workspace(session.id.as_str())
                .map(|path| project_key_from_path(&path))
        })
}

fn latest_user_query_text(session: &Session) -> Option<String> {
    session
        .messages
        .iter()
        .rev()
        .find(|message| matches!(message.role, crate::agent::core::Role::User))
        .map(|message| message.content.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

async fn load_session_note_snippets(memory: &MemoryStore, session_id: &str) -> Vec<TopicSnippet> {
    let topics = match memory.list_session_topics(session_id).await {
        Ok(t) => t,
        Err(error) => {
            tracing::warn!("[{}] Failed to list memory topics: {}", session_id, error);
            Vec::new()
        }
    };

    let mut snippets = Vec::new();
    let mut total_chars = 0usize;

    for topic in &topics {
        let content = match memory.read_session_topic(session_id, topic).await {
            Ok(Some(c)) => c.trim().to_string(),
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(
                    "[{}] Failed to read session topic '{}': {}",
                    session_id,
                    topic,
                    error
                );
                continue;
            }
        };
        if content.is_empty() {
            continue;
        }
        let full_len = count_chars(&content);
        let remaining = SESSION_NOTE_PROMPT_MAX_TOTAL_CHARS.saturating_sub(total_chars);
        let cap = remaining.min(SESSION_NOTE_PROMPT_MAX_CHARS_PER_TOPIC);
        if cap == 0 {
            snippets.push(TopicSnippet {
                name: topic.clone(),
                content: String::new(),
                truncated: true,
                full_len,
            });
            continue;
        }
        let (snippet, truncated) = truncate_chars(&content, cap);
        total_chars += count_chars(&snippet);
        snippets.push(TopicSnippet {
            name: topic.clone(),
            content: snippet,
            truncated,
            full_len,
        });
    }

    snippets
}

async fn load_project_memory_index_snippet(
    memory: &MemoryStore,
    session_id: &str,
    project_key: Option<&str>,
) -> Option<ProjectMemoryIndexSnippet> {
    let project_key = project_key?.trim();
    if project_key.is_empty() {
        return None;
    }

    let content = match memory
        .read_memory_view(MemoryScope::Project, Some(project_key))
        .await
    {
        Ok(Some(content)) => content,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to read project memory view for '{}': {}",
                session_id,
                project_key,
                error
            );
            return None;
        }
    };

    let full_len = count_chars(&content);
    let (snippet, truncated) = truncate_chars(&content, PROJECT_MEMORY_INDEX_PROMPT_MAX_CHARS);
    let freshness_note = extract_latest_updated_at_from_memory_view(&content)
        .and_then(|updated_at| render_memory_freshness_note(&updated_at, FreshnessKind::Index));

    Some(ProjectMemoryIndexSnippet {
        project_key: project_key.to_string(),
        content: snippet,
        truncated,
        full_len,
        freshness_note,
    })
}

async fn load_relevant_memory_snippets(
    session: &Session,
    memory: &MemoryStore,
    session_id: &str,
    project_key: Option<&str>,
) -> Vec<RelevantMemorySnippet> {
    let Some(query) = latest_user_query_text(session) else {
        return Vec::new();
    };

    let candidates = match shortlist_relevant_memories(
        memory,
        project_key,
        &query,
        &MemoryRecallOptions {
            shortlist_limit: RELEVANT_MEMORY_RESULT_LIMIT,
            include_global_fallback: true,
            max_candidates_per_scope: RELEVANT_MEMORY_RESULT_LIMIT.max(12),
        },
    )
    .await
    {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to shortlist relevant durable memories: {}",
                session_id,
                error
            );
            return Vec::new();
        }
    };

    let mut rendered = Vec::new();
    let mut total_chars = 0usize;

    for candidate in candidates {
        let Some(snippet) =
            build_relevant_memory_snippet(candidate, RELEVANT_MEMORY_PER_ITEM_MAX_CHARS)
        else {
            continue;
        };

        let estimated_len = count_chars(&snippet.summary)
            + snippet
                .freshness_note
                .as_deref()
                .map(count_chars)
                .unwrap_or(0)
            + count_chars(&snippet.title)
            + 48;
        if total_chars + estimated_len > RELEVANT_MEMORY_TOTAL_MAX_CHARS && !rendered.is_empty() {
            break;
        }
        total_chars += estimated_len;
        rendered.push(snippet);
    }

    rendered
}

fn build_relevant_memory_snippet(
    candidate: MemoryRecallCandidate,
    per_item_max_chars: usize,
) -> Option<RelevantMemorySnippet> {
    let (summary, truncated) = memory_truncate_chars(candidate.summary.trim(), per_item_max_chars);
    let summary = if truncated {
        format!("{}...", summary.trim_end())
    } else {
        summary.trim().to_string()
    };
    if summary.is_empty() {
        return None;
    }

    Some(RelevantMemorySnippet {
        title: candidate.title,
        scope: candidate.scope,
        status: candidate.status.as_str().to_string(),
        score: candidate.score,
        summary,
        freshness_note: render_memory_freshness_note(
            &candidate.updated_at,
            FreshnessKind::RecalledMemory,
        ),
    })
}

async fn load_project_dream_snippet(
    memory: &MemoryStore,
    session_id: &str,
    project_key: Option<&str>,
) -> Option<ProjectDreamSnippet> {
    let project_key = project_key?.trim();
    if project_key.is_empty() {
        return None;
    }

    let content = match memory.read_project_dream_view(project_key).await {
        Ok(Some(content)) => content,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to read project Dream notebook for '{}': {}",
                session_id,
                project_key,
                error
            );
            return None;
        }
    };

    let full_len = count_chars(&content);
    let (snippet, truncated) = truncate_chars(&content, GLOBAL_DREAM_NOTEBOOK_PROMPT_MAX_CHARS);
    Some(ProjectDreamSnippet {
        project_key: project_key.to_string(),
        content: snippet,
        truncated,
        full_len,
    })
}

async fn load_global_dream_fallback_snippet(
    memory: &MemoryStore,
    session_id: &str,
) -> Option<LoadedSnippet> {
    let content = match memory.read_dream_view().await {
        Ok(Some(content)) => content,
        Ok(None) => return None,
        Err(error) => {
            tracing::warn!("[{}] Failed to read Dream notebook: {}", session_id, error);
            return None;
        }
    };

    let full_len = count_chars(&content);
    let (snippet, truncated) = truncate_chars(&content, GLOBAL_DREAM_NOTEBOOK_PROMPT_MAX_CHARS);
    Some(LoadedSnippet {
        content: snippet,
        truncated,
        full_len,
    })
}

fn extract_latest_updated_at_from_memory_view(content: &str) -> Option<String> {
    content.lines().find_map(|line| {
        let line = line.trim();
        if !line.starts_with("- `") {
            return None;
        }
        let (_, updated_at) = line.rsplit_once(" updated ")?;
        let updated_at = updated_at.trim();
        (!updated_at.is_empty()).then(|| updated_at.to_string())
    })
}

fn build_external_memory_section(
    session: &Session,
    session_note_snippets: &[TopicSnippet],
    project_memory_index: Option<&ProjectMemoryIndexSnippet>,
    relevant_memory_snippets: &[RelevantMemorySnippet],
    project_dream: Option<&ProjectDreamSnippet>,
    global_dream_fallback: Option<&LoadedSnippet>,
) -> String {
    let mut section = String::new();
    section.push_str("\n\n");
    section.push_str(EXTERNAL_MEMORY_START_MARKER);
    section.push('\n');
    section.push_str("## External Memory (Persistent)\n\n");
    section.push_str("You have access to layered persistent memory for this conversation:\n");
    section.push_str(
        "- **Session Memory Note**: current-session continuity for this session/workstream\n",
    );
    section.push_str(
        "- **Relevant Durable Memories**: turn-specific historical memories shortlisted for the current user request\n",
    );
    section.push_str(
        "- **Project Durable Memory Index**: canonical cross-session project memory when project scope is confidently known\n",
    );
    section.push_str(
        "- **Project Dream Summary**: synthesized project-scoped orientation when available\n",
    );
    section.push_str(
        "- **Dream Summaries**: synthesized orientation only, lower-trust than durable memory and current observed state\n\n",
    );
    section.push_str(
        "Priority order for decisions: current observed state from tools/files > session note > relevant durable memories > project durable memory index > project Dream > global Dream fallback.\n",
    );
    section.push_str(
        "If indexed or recalled memory appears to describe files, symbols, configs, or runtime state, verify it against current tools/files before asserting it as fact.\n\n",
    );
    section.push_str(
        "Use the `memory` tool for durable project/global knowledge that should persist across sessions.\n\n",
    );
    section.push_str("- If you learn durable information that will help later in other sessions (preferences, confirmed project decisions, stable references, non-derivable context), store it with the `memory` tool instead of only leaving it in session_note.\n");
    section.push_str("- For durable memory, prefer `memory` action=query first, then `memory` action=get for the specific item you need, and use `memory` action=write/merge only when the fact should become canonical memory.\n");
    section.push_str("- Do NOT store secrets/tokens.\n");
    section.push_str(
        "- Keep the session note concise and factual. If it gets too long, compress it (rewrite a shorter version) and replace it.\n\n",
    );
    section.push_str("Session-memory tool usage:\n");
    section.push_str(&format!(
        "- Append: call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"append\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Replace (for compression): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"replace\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Read full note (if truncated): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"read\"}}`\n"
    ));
    section.push_str(
        "- Use separate topics: add `\"topic\":\"my-topic\"` to keep unrelated workstreams isolated\n",
    );
    section.push_str(&format!(
        "- List topics: call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"list_topics\"}}`\n\n"
    ));

    section.push_str(&render_session_note_section(session_note_snippets));

    if !relevant_memory_snippets.is_empty() {
        section.push_str(&render_relevant_memory_section(relevant_memory_snippets));
    }

    if let Some(project_memory_index) = project_memory_index {
        section.push_str(&render_project_memory_index_section(project_memory_index));
    }

    if let Some(project_dream) = project_dream {
        section.push_str(&render_project_dream_section(project_dream));
    }

    if let Some(global_dream_fallback) = global_dream_fallback {
        section.push_str(&render_global_dream_fallback_section(global_dream_fallback));
    }

    if let Some(context_pressure_warning) = render_context_pressure_warning(session) {
        section.push_str(&context_pressure_warning);
    }

    section.push('\n');
    section.push_str(EXTERNAL_MEMORY_END_MARKER);
    section
}

fn render_session_note_section(snippets: &[TopicSnippet]) -> String {
    let mut section = String::new();

    if snippets.is_empty() {
        section.push_str("### Session Memory Note (markdown)\n");
        section.push_str("````md\n_(empty)_\n````\n\n");
        return section;
    }

    if snippets.len() == 1 && snippets[0].name == "default" {
        let s = &snippets[0];
        section.push_str("### Session Memory Note (markdown)\n");
        section.push_str("````md\n");
        if s.content.is_empty() {
            section.push_str("_(empty)_");
        } else {
            section.push_str(&s.content);
        }
        section.push_str("\n````\n");
        if s.truncated {
            section.push_str(&format!(
                "\nNote is truncated in the system prompt (showing first {} chars of {}). Use `{}` action=read to view it and then action=replace to compress it.\n\n",
                count_chars(&s.content),
                s.full_len,
                EXTERNAL_MEMORY_TOOL_NAME
            ));
        } else {
            section.push('\n');
        }
        return section;
    }

    for s in snippets {
        section.push_str(&format!("### Session Memory Topic: `{}`\n", s.name));
        section.push_str("````md\n");
        if s.content.is_empty() {
            section.push_str("_(truncated — use action=read topic=");
            section.push_str(&s.name);
            section.push_str(" to view)_");
        } else {
            section.push_str(&s.content);
        }
        section.push_str("\n````\n");
        if s.truncated && !s.content.is_empty() {
            section.push_str(&format!(
                "_(showing {} of {} chars — use action=read topic={} to see full content)_\n",
                count_chars(&s.content),
                s.full_len,
                s.name
            ));
        }
    }
    section.push('\n');
    section
}

fn render_relevant_memory_section(snippets: &[RelevantMemorySnippet]) -> String {
    let mut section = String::new();
    section.push_str("### Relevant Durable Memories\n");
    section.push_str(
        "Turn-specific historical memories shortlisted for the latest user request. Verify them against current tools/files before treating them as live state.\n",
    );

    for snippet in snippets {
        section.push_str(&format!(
            "- [{}][{}] {} (score {:.2})\n",
            snippet.status,
            snippet.scope.as_str(),
            snippet.title,
            snippet.score
        ));
        section.push_str(&format!("  Summary: {}\n", snippet.summary));
        if let Some(note) = snippet.freshness_note.as_deref() {
            section.push_str(&format!("  {}\n", note));
        }
    }
    section.push('\n');
    section
}

fn render_project_memory_index_section(snippet: &ProjectMemoryIndexSnippet) -> String {
    let mut section = String::new();
    section.push_str("### Project Durable Memory Index\n");
    section.push_str(&format!(
        "Canonical cross-session memory for project `{}`.\n",
        snippet.project_key
    ));
    section.push_str("````md\n");
    section.push_str(&snippet.content);
    section.push_str("\n````\n");
    if snippet.truncated {
        section.push_str(&format!(
            "_(showing {} of {} chars from the current project's durable memory index)_\n",
            count_chars(&snippet.content),
            snippet.full_len,
        ));
    }
    if let Some(note) = snippet.freshness_note.as_deref() {
        section.push_str(note);
        section.push('\n');
    }
    section.push('\n');
    section
}

fn render_project_dream_section(snippet: &ProjectDreamSnippet) -> String {
    let mut section = String::new();
    section.push_str("### Project Dream Summary\n");
    section.push_str(&format!(
        "Synthesized project-scoped orientation for `{}`. This is lower-trust than durable memory and not authoritative current truth.\n",
        snippet.project_key
    ));
    section.push_str("````md\n");
    section.push_str(&snippet.content);
    section.push_str("\n````\n");
    if snippet.truncated {
        section.push_str(&format!(
            "_(showing {} of {} chars from the project Dream notebook)_\n",
            count_chars(&snippet.content),
            snippet.full_len,
        ));
    }
    section.push_str(
        "Verify against current tools/files before relying on Dream content for repository state or project-specific claims.\n\n",
    );
    section
}

fn render_global_dream_fallback_section(snippet: &LoadedSnippet) -> String {
    let mut section = String::new();
    section.push_str("### Global Dream Summary (fallback)\n");
    section.push_str(
        "Synthesized auxiliary orientation only. This may be broader than the current project and is not authoritative current truth.\n",
    );
    section.push_str("````md\n");
    section.push_str(&snippet.content);
    section.push_str("\n````\n");
    if snippet.truncated {
        section.push_str(&format!(
            "_(showing {} of {} chars from the global Dream notebook fallback)_\n",
            count_chars(&snippet.content),
            snippet.full_len,
        ));
    }
    section.push_str(
        "Verify against current tools/files before relying on Dream content for repository state or project-specific claims.\n\n",
    );
    section
}

fn render_context_pressure_warning(session: &Session) -> Option<String> {
    let usage = session.token_usage.as_ref()?;
    let denominator = if usage.max_context_tokens > 0 {
        usage.max_context_tokens
    } else {
        usage.budget_limit
    };
    if denominator == 0 {
        return None;
    }

    let pct = (usage.total_tokens as f64 / denominator as f64) * 100.0;
    if pct < CONTEXT_PRESSURE_WARNING_THRESHOLD {
        return None;
    }

    Some(format!(
        "\n> ⚠️ **Context window filling up (~{pct:.0}% used).** Older messages will soon be compressed and summarized. Save any important context (key decisions, file paths, architecture notes, progress state) to `{EXTERNAL_MEMORY_TOOL_NAME}` now so it persists across the compression boundary.\n"
    ))
}
