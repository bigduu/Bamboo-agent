use std::path::Path;
use std::sync::Arc;

use crate::runtime::config::PromptMemoryFlags;
use bamboo_agent_core::{PromptMemoryObservability, Session};
use bamboo_domain::ledger::LedgerScope;
use bamboo_llm::LLMProvider;
use bamboo_memory::ledger_store::{AgendaItem, AgendaSnapshot, LedgerStore};
use bamboo_memory::memory_store::{
    project_key_from_path, render_memory_freshness_note, select_relevant_memories,
    truncate_chars as memory_truncate_chars, FreshnessKind, MemoryRecallCandidate,
    MemoryRecallOptions, MemoryRecallRerankContext, MemoryRecallStrategy, MemoryScope, MemoryStore,
};
use bamboo_tools::tools::workspace_state;

use super::system_sections::strip_existing_prompt_block;

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
/// Session metadata key holding the rendered external-memory section body
/// (marker-free). Written each round by the async refresh; read by the request
/// assembler to build the volatile `ExternalMemory` block. External memory is the
/// one ASYNC volatile producer, so it is computed once per round and cached here
/// rather than reparsed from the system message.
pub(crate) const EXTERNAL_MEMORY_RENDERED_KEY: &str = "external_memory_rendered";
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
/// Max chars for the ledger agenda section.
const LEDGER_AGENDA_PROMPT_MAX_CHARS: usize = 1_200;
/// Max items rendered per agenda bucket (overdue/today/upcoming/undated).
const LEDGER_AGENDA_ITEMS_PER_BUCKET: usize = 5;
/// Days ahead the injected agenda looks.
const LEDGER_AGENDA_HORIZON_DAYS: i64 = 7;
const EXTERNAL_MEMORY_TOOL_NAME: &str = "session_note";
pub(crate) const PROMPT_MEMORY_OBSERVABILITY_KEY: &str = "runtime_prompt_memory_observability";

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

#[derive(Clone)]
struct RelevantMemoryLoadResult {
    snippets: Vec<RelevantMemorySnippet>,
    strategy: MemoryRecallStrategy,
}

#[derive(Debug, Clone)]
struct LedgerAgendaSnippet {
    content: String,
    item_count: usize,
}

#[derive(Clone)]
pub(crate) struct PromptMemoryRuntimeContext {
    pub llm: Arc<dyn LLMProvider>,
    pub background_model_name: Option<String>,
}

#[derive(Debug, Clone)]
struct ExternalMemoryRenderParts {
    session_note_section: String,
    #[allow(dead_code)]
    ledger_agenda_section: String,
    relevant_memory_section: String,
    project_memory_index_section: String,
    project_dream_section: String,
    global_dream_fallback_section: String,
    context_pressure_warning: String,
    full_section: String,
}

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
}

/// Extract the marker-free body from a freshly-rendered external-memory section.
fn extract_external_memory_inner(full_section: &str) -> String {
    full_section
        .trim()
        .strip_prefix(EXTERNAL_MEMORY_START_MARKER)
        .and_then(|rest| rest.strip_suffix(EXTERNAL_MEMORY_END_MARKER))
        .map(|inner| inner.trim().to_string())
        .unwrap_or_default()
}

/// The rendered external-memory section body for this round (marker-free), or
/// `None` when there is nothing to surface. Read from the session field populated
/// by the async refresh.
pub(crate) fn rendered_external_memory_section(session: &Session) -> Option<String> {
    let content = session.metadata.get(EXTERNAL_MEMORY_RENDERED_KEY)?;
    let trimmed = content.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
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

pub(super) async fn refresh_external_memory_context(
    session: &mut Session,
    prompt_memory_flags: PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    let memory = MemoryStore::with_defaults();
    refresh_external_memory_context_with_store(
        session,
        &memory,
        prompt_memory_flags,
        runtime_context,
    )
    .await;
}

pub(super) async fn refresh_external_memory_context_with_store(
    session: &mut Session,
    memory: &MemoryStore,
    prompt_memory_flags: PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    // Computed each round and cached in a session field (NOT injected into the
    // system message), so a per-round memory change never invalidates the cached
    // system prefix; the request assembler reads the field to build a volatile
    // block.
    let session_id = session.id.clone();
    let resolved_project_key = resolve_prompt_project_key(session);
    let session_note_snippets = load_session_note_snippets(memory, session_id.as_str()).await;
    let ledger_agenda = if prompt_memory_flags.ledger_agenda {
        load_ledger_agenda_snippet(memory, resolved_project_key.as_deref()).await
    } else {
        None
    };
    let project_memory_index = if prompt_memory_flags.project_prompt_injection {
        load_project_memory_index_snippet(
            memory,
            session_id.as_str(),
            resolved_project_key.as_deref(),
        )
        .await
    } else {
        None
    };
    let relevant_memory_result = if prompt_memory_flags.relevant_recall {
        load_relevant_memory_snippets(
            session,
            memory,
            session_id.as_str(),
            resolved_project_key.as_deref(),
            prompt_memory_flags,
            runtime_context,
        )
        .await
    } else {
        RelevantMemoryLoadResult {
            snippets: Vec::new(),
            strategy: MemoryRecallStrategy::Lexical,
        }
    };
    let relevant_memory_snippets = relevant_memory_result.snippets.clone();
    let project_dream = if prompt_memory_flags.project_first_dream {
        load_project_dream_snippet(memory, session_id.as_str(), resolved_project_key.as_deref())
            .await
    } else {
        None
    };
    let global_dream_fallback = if prompt_memory_flags.project_first_dream {
        if project_dream.is_none() && project_memory_index.is_none() {
            load_global_dream_fallback_snippet(memory, session_id.as_str()).await
        } else {
            None
        }
    } else {
        load_global_dream_fallback_snippet(memory, session_id.as_str()).await
    };

    let latest_user_query_present = latest_user_query_text(session).is_some();
    let render_parts = build_external_memory_render_parts(
        session,
        &session_note_snippets,
        ledger_agenda.as_ref(),
        project_memory_index.as_ref(),
        &relevant_memory_snippets,
        project_dream.as_ref(),
        global_dream_fallback.as_ref(),
    );
    if let Some(agenda) = &ledger_agenda {
        tracing::debug!(
            "[{}] Ledger agenda injected: items={}, chars={}",
            session_id,
            agenda.item_count,
            count_chars(&agenda.content),
        );
    }
    let observability = build_prompt_memory_observability(
        prompt_memory_flags,
        resolved_project_key.clone(),
        latest_user_query_present,
        &session_note_snippets,
        project_memory_index.as_ref(),
        &relevant_memory_snippets,
        relevant_memory_result.strategy,
        project_dream.as_ref(),
        global_dream_fallback.as_ref(),
        &render_parts,
    );

    let inner = extract_external_memory_inner(&render_parts.full_section);
    if inner.trim().is_empty() {
        session.metadata.remove(EXTERNAL_MEMORY_RENDERED_KEY);
    } else {
        session
            .metadata
            .insert(EXTERNAL_MEMORY_RENDERED_KEY.to_string(), inner);
    }
    persist_prompt_memory_observability(session, &observability);

    tracing::info!(
        "[{}] External memory injected: project_key_resolved={}, project_index_loaded={}, project_index_chars={}, relevant_query_present={}, relevant_count={}, project_dream_loaded={}, project_dream_chars={}, global_dream_fallback_used={}, global_dream_chars={}, session_topics={}, session_note_chars={}, truncated_topics={}, dream_source={}, external_memory_section_chars={}",
        session_id,
        resolved_project_key.as_deref().unwrap_or(""),
        project_memory_index.is_some(),
        project_memory_index
            .as_ref()
            .map(|snippet| count_chars(&snippet.content))
            .unwrap_or(0),
        latest_user_query_present,
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
        observability.dream_source,
        observability.external_memory_section_chars,
    );
}

/// Load the agenda from the ledger store colocated with the memory store's
/// data dir (global scope + the resolved project scope). Pure index/file
/// reads — no LLM cost. `None` when the ledger has nothing open.
async fn load_ledger_agenda_snippet(
    memory: &MemoryStore,
    project_key: Option<&str>,
) -> Option<LedgerAgendaSnippet> {
    let store = LedgerStore::new(memory.resolver().data_dir());
    let mut scopes: Vec<(LedgerScope, Option<String>)> = vec![(LedgerScope::Global, None)];
    if let Some(project_key) = project_key {
        scopes.push((LedgerScope::Project, Some(project_key.to_string())));
    }
    let snapshot = store
        .agenda(&scopes, chrono::Utc::now(), LEDGER_AGENDA_HORIZON_DAYS)
        .await
        .ok()?;
    if snapshot.is_empty() {
        return None;
    }
    let item_count = snapshot.overdue.len()
        + snapshot.today.len()
        + snapshot.upcoming.len()
        + snapshot.undated.len();
    Some(LedgerAgendaSnippet {
        content: render_ledger_agenda_lines(&snapshot),
        item_count,
    })
}

fn render_ledger_agenda_lines(snapshot: &AgendaSnapshot) -> String {
    fn push_bucket(out: &mut String, label: &str, items: &[AgendaItem]) {
        for item in items.iter().take(LEDGER_AGENDA_ITEMS_PER_BUCKET) {
            let when = item
                .anchor_at
                .map(|at| format!(" — {}", at.format("%Y-%m-%d %H:%M UTC")))
                .unwrap_or_default();
            out.push_str(&format!(
                "- [{label}] `{}` ({}) {}{}\n",
                item.id,
                item.kind.as_str(),
                item.title,
                when,
            ));
        }
        let hidden = items.len().saturating_sub(LEDGER_AGENDA_ITEMS_PER_BUCKET);
        if hidden > 0 {
            out.push_str(&format!("- [{label}] …and {hidden} more\n"));
        }
    }

    let mut out = String::new();
    push_bucket(&mut out, "OVERDUE", &snapshot.overdue);
    push_bucket(&mut out, "NEXT 24H", &snapshot.today);
    push_bucket(&mut out, "UPCOMING", &snapshot.upcoming);
    push_bucket(&mut out, "OPEN", &snapshot.undated);
    out
}

fn render_ledger_agenda_section(snippet: &LedgerAgendaSnippet) -> String {
    let mut section = String::new();
    section.push_str("### Ledger Agenda (prospective records)\n");
    section.push_str(
        "The user's open commitments from the persistent ledger — surface anything urgent \
         proactively when relevant. Times are UTC.\n",
    );
    let (content, truncated) =
        memory_truncate_chars(&snippet.content, LEDGER_AGENDA_PROMPT_MAX_CHARS);
    section.push_str(&content);
    if truncated {
        section.push_str("\n_(agenda truncated; use `ledger` action=agenda for the full view)_\n");
    }
    section.push('\n');
    section
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
        .find(|message| matches!(message.role, bamboo_agent_core::Role::User))
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
    prompt_memory_flags: PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) -> RelevantMemoryLoadResult {
    let Some(query) = latest_user_query_text(session) else {
        return RelevantMemoryLoadResult {
            snippets: Vec::new(),
            strategy: MemoryRecallStrategy::Lexical,
        };
    };

    let rerank_context = if prompt_memory_flags.relevant_recall_rerank {
        runtime_context.and_then(|ctx| {
            ctx.background_model_name
                .as_deref()
                .map(str::trim)
                .filter(|model| !model.is_empty())
                .map(|model| MemoryRecallRerankContext {
                    llm: ctx.llm.clone(),
                    model: model.to_string(),
                    session_id: Some(session_id.to_string()),
                })
        })
    } else {
        None
    };

    let selection = match select_relevant_memories(
        memory,
        project_key,
        &query,
        &MemoryRecallOptions {
            shortlist_limit: RELEVANT_MEMORY_RESULT_LIMIT,
            include_global_fallback: true,
            max_candidates_per_scope: RELEVANT_MEMORY_RESULT_LIMIT.max(12),
        },
        rerank_context.as_ref(),
    )
    .await
    {
        Ok(selection) => selection,
        Err(error) => {
            tracing::warn!(
                "[{}] Failed to select relevant durable memories: {}",
                session_id,
                error
            );
            return RelevantMemoryLoadResult {
                snippets: Vec::new(),
                strategy: MemoryRecallStrategy::Lexical,
            };
        }
    };

    let mut rendered = Vec::new();
    let mut total_chars = 0usize;

    for candidate in selection.candidates {
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

    RelevantMemoryLoadResult {
        snippets: rendered,
        strategy: selection.strategy,
    }
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

fn build_external_memory_render_parts(
    session: &Session,
    session_note_snippets: &[TopicSnippet],
    ledger_agenda: Option<&LedgerAgendaSnippet>,
    project_memory_index: Option<&ProjectMemoryIndexSnippet>,
    relevant_memory_snippets: &[RelevantMemorySnippet],
    project_dream: Option<&ProjectDreamSnippet>,
    global_dream_fallback: Option<&LoadedSnippet>,
) -> ExternalMemoryRenderParts {
    let session_note_section = render_session_note_section(session_note_snippets);
    let ledger_agenda_section = ledger_agenda
        .map(render_ledger_agenda_section)
        .unwrap_or_default();
    let relevant_memory_section = if relevant_memory_snippets.is_empty() {
        String::new()
    } else {
        render_relevant_memory_section(relevant_memory_snippets)
    };
    let project_memory_index_section = project_memory_index
        .map(render_project_memory_index_section)
        .unwrap_or_default();
    let project_dream_section = project_dream
        .map(render_project_dream_section)
        .unwrap_or_default();
    let global_dream_fallback_section = global_dream_fallback
        .map(render_global_dream_fallback_section)
        .unwrap_or_default();
    let context_pressure_warning = render_context_pressure_warning(session).unwrap_or_default();

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
        "- **Ledger Agenda**: the user's open prospective records (todos, events, reminders) that are overdue or coming up\n",
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
        "- **Global Dream Summary (fallback)**: synthesized auxiliary orientation, lower-trust than durable memory and current observed state\n\n",
    );
    section.push_str(
        "Priority order for decisions: current observed state from tools/files > session note > ledger agenda > relevant durable memories > project durable memory index > project Dream > global Dream fallback. This order reflects working-context recency, not factual authority: a session note ranks high because it is the live workstream, so if it conflicts with canonical durable/project memory on an established fact, verify before preferring the note.\n",
    );
    section.push_str(
        "If indexed or recalled memory appears to describe files, symbols, configs, or runtime state, verify it against current tools/files before asserting it as fact.\n\n",
    );
    section.push_str(
        "Two distinct surfaces: use the `session_note` tool for current-session notes (usage shown below), and the `memory` tool only for durable project/global knowledge that should persist across sessions.\n\n",
    );
    section.push_str("- If you learn durable information that will help later in other sessions (preferences, confirmed project decisions, stable references, non-derivable context), store it with the `memory` tool instead of only leaving it in session_note.\n");
    section.push_str("- When the user states a commitment, deadline, appointment, or recurring routine, record it with the `ledger` tool (action=upsert; set due_at/remind_at so reminders actually fire) instead of keeping it only in the session task list. Mark records done/cancelled with action=transition, and answer \"what's on my plate\" questions from action=agenda/query.\n");
    section.push_str("- Proactively recall: when the user refers to their own preferences, past decisions, or subjective/personal context you don't already know — including first-person questions about themselves ('what do I...', 'did I...', '我...?') — call `memory` action=query BEFORE answering. Do not reply that you don't know about the user's own preferences, history, or prior decisions without first querying memory; the auto-injected memories above are only a keyword-matched shortlist and may have missed it.\n");
    section.push_str("- For durable memory, prefer `memory` action=query first, then `memory` action=get for the specific item you need, and use `memory` action=write/merge only when the fact should become canonical memory.\n");
    section.push_str("- One memory = one fact/decision/preference. Do not bundle unrelated facts into a single memory.\n");
    section.push_str("- Give each durable memory a specific, descriptive title that summarizes its own content; recall is keyword-based, so a misleading title makes the memory unfindable.\n");
    section.push_str("- Query before writing: if a memory about the same fact already exists, update or merge it instead of creating a near-duplicate. Only merge content that is the SAME fact — never append an unrelated fact to an existing memory.\n");
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

    section.push_str(&session_note_section);
    section.push_str(&ledger_agenda_section);
    section.push_str(&relevant_memory_section);
    section.push_str(&project_memory_index_section);
    section.push_str(&project_dream_section);
    section.push_str(&global_dream_fallback_section);
    section.push_str(&context_pressure_warning);
    section.push('\n');
    section.push_str(EXTERNAL_MEMORY_END_MARKER);

    ExternalMemoryRenderParts {
        session_note_section,
        ledger_agenda_section,
        relevant_memory_section,
        project_memory_index_section,
        project_dream_section,
        global_dream_fallback_section,
        context_pressure_warning,
        full_section: section,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_prompt_memory_observability(
    prompt_memory_flags: PromptMemoryFlags,
    resolved_project_key: Option<String>,
    latest_user_query_present: bool,
    session_note_snippets: &[TopicSnippet],
    project_memory_index: Option<&ProjectMemoryIndexSnippet>,
    relevant_memory_snippets: &[RelevantMemorySnippet],
    relevant_memory_strategy: MemoryRecallStrategy,
    project_dream: Option<&ProjectDreamSnippet>,
    global_dream_fallback: Option<&LoadedSnippet>,
    render_parts: &ExternalMemoryRenderParts,
) -> PromptMemoryObservability {
    let session_notes_status = if session_note_snippets.is_empty() {
        "empty"
    } else if session_note_snippets
        .iter()
        .any(|snippet| snippet.truncated)
    {
        "loaded_truncated"
    } else {
        "loaded"
    };
    let project_memory_index_status = if !prompt_memory_flags.project_prompt_injection {
        "disabled"
    } else if let Some(snippet) = project_memory_index {
        if snippet.truncated {
            "loaded_truncated"
        } else {
            "loaded"
        }
    } else if resolved_project_key.is_some() {
        "missing"
    } else {
        "no_project_key"
    };
    let relevant_memory_status = if !prompt_memory_flags.relevant_recall {
        "disabled"
    } else if !latest_user_query_present {
        "no_query"
    } else if relevant_memory_snippets.is_empty() {
        "no_match"
    } else {
        relevant_memory_strategy.as_str()
    };
    let project_dream_status = if !prompt_memory_flags.project_first_dream {
        "disabled"
    } else if let Some(snippet) = project_dream {
        if snippet.truncated {
            "loaded_truncated"
        } else {
            "loaded"
        }
    } else if resolved_project_key.is_some() {
        "missing"
    } else {
        "no_project_key"
    };
    let global_dream_fallback_status = if !prompt_memory_flags.project_first_dream {
        if let Some(snippet) = global_dream_fallback {
            if snippet.truncated {
                "forced_loaded_truncated"
            } else {
                "forced_loaded"
            }
        } else {
            "forced_missing"
        }
    } else if project_dream.is_some() || project_memory_index.is_some() {
        "skipped_project_memory_or_dream_present"
    } else if let Some(snippet) = global_dream_fallback {
        if snippet.truncated {
            "fallback_loaded_truncated"
        } else {
            "fallback_loaded"
        }
    } else {
        "fallback_missing"
    };
    let dream_source = if project_dream.is_some() {
        "project"
    } else if global_dream_fallback.is_some() {
        "global_fallback"
    } else {
        "none"
    };

    PromptMemoryObservability {
        project_prompt_injection_enabled: prompt_memory_flags.project_prompt_injection,
        relevant_recall_enabled: prompt_memory_flags.relevant_recall,
        relevant_recall_rerank_enabled: prompt_memory_flags.relevant_recall_rerank,
        project_first_dream_enabled: prompt_memory_flags.project_first_dream,
        latest_user_query_present,
        resolved_project_key,
        session_notes_status: session_notes_status.to_string(),
        project_memory_index_status: project_memory_index_status.to_string(),
        relevant_memory_status: relevant_memory_status.to_string(),
        project_dream_status: project_dream_status.to_string(),
        global_dream_fallback_status: global_dream_fallback_status.to_string(),
        dream_source: dream_source.to_string(),
        session_topic_count: session_note_snippets.len(),
        truncated_session_topic_count: session_note_snippets
            .iter()
            .filter(|snippet| snippet.truncated)
            .count(),
        relevant_memory_count: relevant_memory_snippets.len(),
        session_note_section_chars: count_chars(&render_parts.session_note_section),
        project_memory_index_section_chars: count_chars(&render_parts.project_memory_index_section),
        relevant_memory_section_chars: count_chars(&render_parts.relevant_memory_section),
        project_dream_section_chars: count_chars(&render_parts.project_dream_section),
        global_dream_fallback_section_chars: count_chars(
            &render_parts.global_dream_fallback_section,
        ),
        context_pressure_warning_chars: count_chars(&render_parts.context_pressure_warning),
        external_memory_section_chars: count_chars(&render_parts.full_section),
    }
}

fn persist_prompt_memory_observability(
    session: &mut Session,
    observability: &PromptMemoryObservability,
) {
    if let Ok(raw) = serde_json::to_string(observability) {
        session
            .metadata
            .insert(PROMPT_MEMORY_OBSERVABILITY_KEY.to_string(), raw);
    }
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
