use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use bamboo_agent_core::{Message, Role, Session, SessionKind};
use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordActor, RecordKind};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::CompressionEventKind;
use bamboo_llm::Config;
use bamboo_llm::ProviderRegistry;
use bamboo_llm::{LLMChunk, LLMProvider, LLMRequestOptions};
use bamboo_memory::auto_dream::{
    build_consolidation_prompt, build_extraction_prompt, build_rebuild_consolidation_prompt,
    derive_session_outline, normalize_dream_notebook_body, parse_candidate_scope,
    parse_candidate_type, parse_extraction_candidates, parse_last_consolidated_at,
    parse_last_full_rebuild_at, parse_ledger_candidates, should_force_full_rebuild, truncate_chars,
    ConsolidationSessionInfo, DreamCandidateInfo, DreamGenerationMode, DurableExtractionCandidate,
    LedgerExtractionCandidate,
};
use bamboo_memory::ledger_store::store::new_record_id;
use bamboo_memory::ledger_store::{LedgerStore, RecordFilter, MAX_RECORD_TITLE_LEN};
use bamboo_memory::memory_store::{
    DurableMemoryStatus, DurableMemoryType, MemoryScope, MemoryStore, MAX_MEMORY_TITLE_LEN,
};
use bamboo_storage::{
    search_index::session_history_search_artifact_ids, SessionIndexEntry, SessionStoreV2,
};

use crate::auto_dream_privacy::{
    durable_candidate_is_secret_safe, extraction_sources_are_secret_safe,
    ledger_candidate_is_secret_safe, sanitize_extraction_source, sanitize_extraction_source_pair,
    REDACTED_EXTRACTION_SOURCE,
};
use crate::project_context::ProjectContextResolver;

const DREAM_RUNTIME_SESSION_ID: &str = "__dream__";
const DREAM_TRACING_TARGET: &str = "bamboo.auto_dream";
// Auto-Dream tick cadence now lives in `MemoryConfig::auto_dream_interval_secs`
// (default 30 min); see `spawn_auto_dream_task`.
const DREAM_FULL_REBUILD_INTERVAL_SECS: i64 = 60 * 60 * 24 * 30;
const DREAM_MAX_SESSIONS: usize = 12;
const DREAM_MAX_SUMMARY_CHARS: usize = 12_000;
const EXTRACTION_MAX_TOPICS_PER_SESSION: usize = 4;
const EXTRACTION_MAX_TOPIC_CHARS: usize = 1_500;
const EXTRACTION_MAX_CANDIDATES: usize = 8;
const EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH: usize = 32;
const EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH: usize =
    EXTRACTION_MAX_CANDIDATES * EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH;
const RETRIEVAL_EXTRACTION_MAX_SOURCE_ITEMS: usize = EXTRACTION_MAX_CANDIDATES;
const RETRIEVAL_EXTRACTION_MAX_CHARS: usize = 12_000;
const RETRIEVAL_EXTRACTION_CONTENT_SEGMENT_CHARS: usize = 1_500;
const RETRIEVAL_EXTRACTION_CONTINUATION_OVERLAP_CHARS: usize = 128;
const RETRIEVAL_EXTRACTION_HEADER_RESERVE_CHARS: usize = 1_536;
const EXTRACTION_CHECKPOINT_VERSION: u32 = 3;
const EXTRACTION_CHECKPOINT_DIR: &str = "auto_dream/extraction-checkpoints/v3";
const RETRIEVAL_SOURCE_STATE_VERSION: u32 = 1;

fn provider_session_alias(index: usize) -> String {
    format!("source-session-{:04}", index + 1)
}

fn provider_project_alias(index: usize) -> String {
    format!("source-project-{:04}", index + 1)
}

fn restore_provider_session_alias(
    session_id: &mut Option<String>,
    provider_aliases: &HashMap<String, String>,
) -> bool {
    let Some(alias) = session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    let Some(authoritative) = provider_aliases.get(alias) else {
        return false;
    };
    *session_id = Some(authoritative.clone());
    true
}

fn sanitize_title_and_optional_source(
    title: &str,
    source: Option<&str>,
) -> (String, Option<String>) {
    match source {
        Some(source) => {
            let (title, source) = sanitize_extraction_source_pair(title, source);
            (title, Some(source))
        }
        None => (sanitize_extraction_source(title), None),
    }
}

fn to_consolidation_sessions(
    entries: &[(SessionIndexEntry, Option<String>)],
) -> Vec<ConsolidationSessionInfo> {
    entries
        .iter()
        .enumerate()
        .map(|(index, (entry, summary))| {
            let mut sources = vec![entry.title.as_str()];
            if let Some(last_run_status) = entry.last_run_status.as_deref() {
                sources.push(last_run_status);
            }
            if let Some(summary) = summary.as_deref() {
                sources.push(summary);
            }
            if !extraction_sources_are_secret_safe(&sources) {
                return ConsolidationSessionInfo {
                    id: provider_session_alias(index),
                    title: REDACTED_EXTRACTION_SOURCE.to_string(),
                    kind: format!("{:?}", entry.kind),
                    updated_at: entry.updated_at.to_rfc3339(),
                    message_count: entry.message_count,
                    last_run_status: None,
                    summary: None,
                };
            }

            let (title, summary) =
                sanitize_title_and_optional_source(&entry.title, summary.as_deref());
            ConsolidationSessionInfo {
                id: provider_session_alias(index),
                title,
                kind: format!("{:?}", entry.kind),
                updated_at: entry.updated_at.to_rfc3339(),
                message_count: entry.message_count,
                last_run_status: entry.last_run_status.clone(),
                summary,
            }
        })
        .collect()
}

#[derive(Clone)]
pub struct AutoDreamContext {
    pub session_store: Arc<SessionStoreV2>,
    pub storage: Arc<dyn bamboo_agent_core::storage::Storage>,
    pub memory: MemoryStore,
    pub provider: Arc<dyn LLMProvider>,
    pub config: Arc<RwLock<Config>>,
    pub provider_registry: Arc<ProviderRegistry>,
}

fn memory_store_for_context(ctx: &AutoDreamContext) -> MemoryStore {
    ctx.memory.clone()
}

fn ledger_store_for_context(ctx: &AutoDreamContext) -> LedgerStore {
    // Ledger remains Bamboo-owned and intentionally stays under Bamboo's root;
    // only durable memory and derived Dream snapshots live in Jiandu.
    LedgerStore::new(ctx.session_store.bamboo_home_dir())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoDreamRunResult {
    pub used_model: String,
    pub session_count: usize,
    pub generated_at: String,
    pub source_generation: String,
    pub notebook_chars: usize,
}

#[derive(Debug, Clone)]
struct CandidateSessionContext {
    entry: SessionIndexEntry,
    summary: Option<String>,
    session_id: String,
    project_key: Option<String>,
    topics: Vec<(String, String)>,
    retrieval_event_key: Option<String>,
}

#[derive(Debug, Clone)]
struct DreamSourceWindow {
    existing_dream: Option<String>,
    durable_memory_index: Option<String>,
    sessions: Vec<(SessionIndexEntry, Option<String>)>,
}

fn collect_json_payload_string_values<'a>(value: &'a serde_json::Value, out: &mut Vec<&'a str>) {
    match value {
        serde_json::Value::String(value) => out.push(value),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_payload_string_values(value, out);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "session_id" | "project_key") {
                    continue;
                }
                collect_json_payload_string_values(value, out);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn task_list_is_secret_safe(task_list: &bamboo_domain::TaskList) -> bool {
    let Ok(serialized) = serde_json::to_value(task_list) else {
        return false;
    };
    let mut sources = Vec::new();
    collect_json_payload_string_values(&serialized, &mut sources);
    extraction_sources_are_secret_safe(&sources)
}

fn derive_sanitized_session_outline(session: &bamboo_agent_core::Session) -> Option<String> {
    // Inspect every complete task-list field, including ordered field pairs,
    // before TaskList::format_for_prompt truncates them. If the serialized
    // shape ever becomes unreadable, fail closed rather than send task data.
    if session.task_list.as_ref().is_some_and(|task_list| {
        !task_list.items.is_empty() && !task_list_is_secret_safe(task_list)
    }) {
        return Some(REDACTED_EXTRACTION_SOURCE.to_string());
    }

    let uses_task_list = session
        .task_list
        .as_ref()
        .is_some_and(|task_list| !task_list.items.is_empty());
    if !uses_task_list {
        let recent_message_sources = session
            .messages
            .iter()
            .rev()
            .filter(|message| {
                matches!(
                    message.role,
                    bamboo_agent_core::Role::User | bamboo_agent_core::Role::Assistant
                )
            })
            .take(6)
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>();
        if !extraction_sources_are_secret_safe(&recent_message_sources) {
            return Some(REDACTED_EXTRACTION_SOURCE.to_string());
        }
    }

    // Sanitize complete message bodies before the outline helper truncates
    // them. Otherwise a credential crossing the 300-character boundary could
    // leave an undetected prefix in the provider prompt.
    let mut sanitized = session.clone();
    for message in &mut sanitized.messages {
        message.content = sanitize_extraction_source(&message.content);
    }
    derive_session_outline(&sanitized).map(|outline| sanitize_extraction_source(&outline))
}

fn sanitized_extraction_candidate_info(
    session: &CandidateSessionContext,
    provider_session_id: String,
    provider_project_key: Option<String>,
) -> DreamCandidateInfo {
    let updated_at = session.entry.updated_at.to_rfc3339();
    let mut sources = vec![session.entry.title.as_str()];
    if let Some(summary) = session.summary.as_deref() {
        sources.push(summary);
    }
    for (topic, content) in &session.topics {
        sources.push(topic);
        sources.push(content);
    }
    if !extraction_sources_are_secret_safe(&sources) {
        return DreamCandidateInfo {
            session_id: provider_session_id,
            title: REDACTED_EXTRACTION_SOURCE.to_string(),
            project_key: provider_project_key.clone(),
            updated_at,
            summary: None,
            topics: Vec::new(),
        };
    }

    let (title, summary) =
        sanitize_title_and_optional_source(&session.entry.title, session.summary.as_deref());
    let topics = session
        .topics
        .iter()
        .map(|(topic, content)| sanitize_extraction_source_pair(topic, content))
        .collect();

    DreamCandidateInfo {
        session_id: provider_session_id,
        title,
        project_key: provider_project_key,
        updated_at,
        summary,
        topics,
    }
}

fn session_is_candidate(entry: &SessionIndexEntry, since: DateTime<Utc>) -> bool {
    matches!(entry.kind, SessionKind::Root)
        && entry.updated_at >= since
        && !entry.id.trim().is_empty()
        && entry.id != DREAM_RUNTIME_SESSION_ID
}

fn session_extraction_sources(
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
    retrieval_source_acknowledged: bool,
) -> Vec<Option<String>> {
    if let Some(summary) = session.conversation_summary.as_ref() {
        return vec![Some(sanitize_extraction_source(&summary.content))];
    }
    if session
        .compression_events
        .iter()
        .any(|event| event.kind == CompressionEventKind::RetrievalWindow)
    {
        let batches = build_retrieval_window_extraction_batches(
            session,
            extraction_watermark,
            retrieval_source_acknowledged,
        );
        return if batches.is_empty() {
            vec![None]
        } else {
            batches.into_iter().map(Some).collect()
        };
    }
    vec![derive_sanitized_session_outline(session)]
}

#[derive(Debug, Clone)]
struct RetrievalExtractionSourceItem {
    source_item_ordinal: usize,
    session_message_ordinal: usize,
    role: &'static str,
    retrieval_event_ordinal: Option<usize>,
    content_segment_ordinal: usize,
    content_segment_count: usize,
    content: String,
}

fn split_retrieval_extraction_content(content: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    for character in content.chars() {
        if current_chars == RETRIEVAL_EXTRACTION_CONTENT_SEGMENT_CHARS {
            chunks.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current.push(character);
        current_chars += 1;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn render_retrieval_extraction_item(item: &RetrievalExtractionSourceItem) -> String {
    let content =
        serde_json::to_string(&item.content).expect("serializing a String as JSON cannot fail");
    format!(
        "\n### Source item {}\n- session_message_ordinal: {}\n- role: {}\n- retrieval_event_ordinal: {}\n- content_segment: {}/{}\n- content: {}\n",
        item.source_item_ordinal,
        item.session_message_ordinal,
        item.role,
        item.retrieval_event_ordinal
            .map(|ordinal| ordinal.to_string())
            .unwrap_or_else(|| "(none)".to_string()),
        item.content_segment_ordinal,
        item.content_segment_count,
        content,
    )
}

fn trailing_chars(content: &str, max_chars: usize) -> String {
    let mut suffix = content.chars().rev().take(max_chars).collect::<Vec<_>>();
    suffix.reverse();
    suffix.into_iter().collect()
}

fn render_retrieval_extraction_overlap(item: &RetrievalExtractionSourceItem) -> String {
    let suffix = trailing_chars(
        &item.content,
        RETRIEVAL_EXTRACTION_CONTINUATION_OVERLAP_CHARS,
    );
    let suffix = serde_json::to_string(&suffix).expect("serializing a String as JSON cannot fail");
    format!(
        "\n## Continuation overlap (context only; duplicated from prior batch)\n- continuation_overlap_source_item_ordinal: {}\n- continuation_overlap_session_message_ordinal: {}\n- continuation_overlap_content_segment: {}/{}\n- continuation_overlap_content_suffix: {}\n",
        item.source_item_ordinal,
        item.session_message_ordinal,
        item.content_segment_ordinal,
        item.content_segment_count,
        suffix,
    )
}

fn session_note_tool_call_ids(session: &Session) -> HashSet<&str> {
    session
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_ref())
        .flatten()
        .filter(|call| bamboo_domain::canonical_tool_name(&call.function.name) == "session_note")
        .map(|call| call.id.as_str())
        .collect()
}

/// Session-note bodies are already supplied through Jiandu Session topics.
/// Tool results may echo arbitrary content, so retrieval extraction retains
/// only fixed non-content acknowledgement fields from a proven session_note
/// call. Every other tool result is excluded.
fn sanitized_session_note_result(
    message: &Message,
    session_note_call_ids: &HashSet<&str>,
) -> Option<String> {
    let call_id = message.tool_call_id.as_deref()?;
    if !session_note_call_ids.contains(call_id) {
        return None;
    }
    let source = serde_json::from_str::<serde_json::Value>(&message.content)
        .ok()?
        .as_object()?
        .clone();
    let action = source.get("action")?.as_str()?;
    if !matches!(
        action,
        "read" | "append" | "replace" | "clear" | "list_topics"
    ) {
        return None;
    }
    let mut safe = serde_json::Map::new();
    safe.insert(
        "tool".to_string(),
        serde_json::Value::String("session_note".to_string()),
    );
    safe.insert(
        "action".to_string(),
        serde_json::Value::String(action.to_string()),
    );
    for field in ["exists", "deleted", "body_truncated"] {
        if let Some(value) = source.get(field).and_then(serde_json::Value::as_bool) {
            safe.insert(field.to_string(), serde_json::Value::Bool(value));
        }
    }
    for field in ["length_chars", "max_chars", "count"] {
        if let Some(value) = source.get(field).and_then(serde_json::Value::as_u64) {
            safe.insert(field.to_string(), serde_json::Value::Number(value.into()));
        }
    }
    Some(serde_json::Value::Object(safe).to_string())
}

fn sanitize_retrieval_extraction_sources(
    eligible_messages: &mut [(usize, &Message, &'static str, String)],
) {
    let selected_sources = eligible_messages
        .iter()
        .map(|(_, _, _, content)| content.as_str())
        .collect::<Vec<_>>();
    if extraction_sources_are_secret_safe(&selected_sources) {
        for (_, _, _, content) in eligible_messages {
            *content = sanitize_extraction_source(content);
        }
        return;
    }

    // Localize common structured credentials without allowing a split secret
    // to escape. Individual fields and adjacent groups of up to four fields
    // cover a value paired with either a complete label or the privacy
    // predicate's two-/three-fragment labels. If removing those groups does
    // not make the complete source safe, fail closed and redact everything.
    let mut unsafe_indexes = HashSet::new();
    for index in 0..selected_sources.len() {
        if !extraction_sources_are_secret_safe(&selected_sources[index..=index]) {
            unsafe_indexes.insert(index);
        }
    }
    let individually_redacted = selected_sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            if unsafe_indexes.contains(&index) {
                ""
            } else {
                source
            }
        })
        .collect::<Vec<_>>();
    if !extraction_sources_are_secret_safe(&individually_redacted) {
        for width in 2..=4 {
            if width > selected_sources.len() {
                break;
            }
            for start in 0..=selected_sources.len() - width {
                let end = start + width;
                if (start..end).any(|index| unsafe_indexes.contains(&index)) {
                    continue;
                }
                if !extraction_sources_are_secret_safe(&selected_sources[start..end]) {
                    unsafe_indexes.extend(start..end);
                }
            }
        }
    }

    let localized = selected_sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            if unsafe_indexes.contains(&index) {
                ""
            } else {
                source
            }
        })
        .collect::<Vec<_>>();
    let can_localize = !unsafe_indexes.is_empty() && extraction_sources_are_secret_safe(&localized);
    for (index, (_, _, _, content)) in eligible_messages.iter_mut().enumerate() {
        *content = if can_localize && !unsafe_indexes.contains(&index) {
            sanitize_extraction_source(content)
        } else {
            REDACTED_EXTRACTION_SOURCE.to_string()
        };
    }
}

fn build_retrieval_window_extraction_batches(
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
    retrieval_source_acknowledged: bool,
) -> Vec<String> {
    let mut retrieval_events = session
        .compression_events
        .iter()
        .filter(|event| event.kind == CompressionEventKind::RetrievalWindow)
        .collect::<Vec<_>>();
    if retrieval_events.is_empty() {
        return Vec::new();
    }
    retrieval_events.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    let eligible_event_ids = retrieval_events
        .iter()
        .filter(|event| extraction_watermark.is_none_or(|watermark| event.created_at > watermark))
        .map(|event| event.id.as_str())
        .collect::<HashSet<_>>();
    let retrieval_event_ordinals = retrieval_events
        .iter()
        .enumerate()
        .map(|(index, event)| (event.id.as_str(), index + 1))
        .collect::<HashMap<_, _>>();
    let history_artifact_ids = session_history_search_artifact_ids(session);
    let session_note_call_ids = session_note_tool_call_ids(session);
    let mut eligible_messages = session
        .messages
        .iter()
        .enumerate()
        .filter_map(|(message_index, message)| {
            if matches!(message.role, Role::System) || history_artifact_ids.contains(&message.id) {
                return None;
            }
            let created_after_watermark =
                extraction_watermark.is_none_or(|watermark| message.created_at > watermark);
            let linked_to_new_retrieval_event = message
                .compressed_by_event_id
                .as_deref()
                .is_some_and(|event_id| eligible_event_ids.contains(event_id));
            // A legacy ordinary-outline watermark never proved coverage of the
            // full transcript. The first retrieval transition therefore reads
            // every canonical non-system content item once. Afterwards, select
            // both messages created after the watermark and older messages
            // newly archived by a later retrieval event.
            if retrieval_source_acknowledged
                && !created_after_watermark
                && !linked_to_new_retrieval_event
            {
                return None;
            }
            let (role, content) = match message.role {
                Role::User => ("user", message.content.clone()),
                Role::Assistant => ("assistant", message.content.clone()),
                Role::Tool => (
                    "tool",
                    sanitized_session_note_result(message, &session_note_call_ids)?,
                ),
                Role::System => return None,
            };
            (!content.trim().is_empty()).then_some((message_index, message, role, content))
        })
        .collect::<Vec<_>>();
    if eligible_messages.is_empty() {
        return Vec::new();
    }

    // Apply the shared privacy predicate across complete source fields before
    // any splitting or truncation. Unsafe fields are localized where that can
    // be proven safe; ambiguous multi-field cases fail closed for the batch.
    sanitize_retrieval_extraction_sources(&mut eligible_messages);

    let mut source_items = Vec::new();
    for (message_index, message, role, extraction_content) in &eligible_messages {
        let segments = split_retrieval_extraction_content(extraction_content);
        let segment_count = segments.len();
        let retrieval_event_ordinal = message
            .compressed_by_event_id
            .as_deref()
            .filter(|event_id| eligible_event_ids.contains(event_id))
            .and_then(|event_id| retrieval_event_ordinals.get(event_id))
            .copied();
        for (segment_index, content) in segments.into_iter().enumerate() {
            source_items.push(RetrievalExtractionSourceItem {
                source_item_ordinal: source_items.len() + 1,
                session_message_ordinal: *message_index + 1,
                role,
                retrieval_event_ordinal,
                content_segment_ordinal: segment_index + 1,
                content_segment_count: segment_count,
                content,
            });
        }
    }

    let max_body_chars =
        RETRIEVAL_EXTRACTION_MAX_CHARS.saturating_sub(RETRIEVAL_EXTRACTION_HEADER_RESERVE_CHARS);
    let mut item_batches: Vec<Vec<(RetrievalExtractionSourceItem, String)>> = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_chars = 0usize;
    for item in source_items {
        let rendered = render_retrieval_extraction_item(&item);
        let rendered_chars = rendered.chars().count();
        debug_assert!(rendered_chars <= max_body_chars);
        if !current_batch.is_empty()
            && (current_batch.len() == RETRIEVAL_EXTRACTION_MAX_SOURCE_ITEMS
                || current_chars.saturating_add(rendered_chars) > max_body_chars)
        {
            item_batches.push(std::mem::take(&mut current_batch));
            current_chars = 0;
        }
        current_chars = current_chars.saturating_add(rendered_chars);
        current_batch.push((item, rendered));
    }
    if !current_batch.is_empty() {
        item_batches.push(current_batch);
    }

    let batch_count = item_batches.len();
    let mut previous_tail = None;
    item_batches
        .into_iter()
        .enumerate()
        .map(|(batch_index, items)| {
            let continuation_overlap = previous_tail
                .as_ref()
                .map(render_retrieval_extraction_overlap);
            previous_tail = items.last().map(|(item, _)| item.clone());
            let distinct_message_count = items
                .iter()
                .map(|(item, _)| item.session_message_ordinal)
                .collect::<HashSet<_>>()
                .len();
            let mut rendered = String::from("# Retrieval-window extraction delta v1\n\n");
            rendered.push_str(&format!(
                "- extraction_watermark: {}\n- batch: {}/{}\n- eligible_retrieval_events: {}\n- eligible_messages: {}\n- source_items_in_batch: {}\n- distinct_messages_in_batch: {}\n- continuation_overlap_items_in_batch: {}\n- max_source_items_per_batch: {}\n- max_characters_per_batch: {}\n- truncated: false\n- continuation: {}\n",
                extraction_watermark
                    .map(|watermark| watermark.to_rfc3339())
                    .unwrap_or_else(|| "(none)".to_string()),
                batch_index + 1,
                batch_count,
                eligible_event_ids.len(),
                eligible_messages.len(),
                items.len(),
                distinct_message_count,
                usize::from(continuation_overlap.is_some()),
                RETRIEVAL_EXTRACTION_MAX_SOURCE_ITEMS,
                RETRIEVAL_EXTRACTION_MAX_CHARS,
                if batch_index + 1 < batch_count {
                    "continues_in_next_batch"
                } else {
                    "final_batch"
                },
            ));
            if let Some(overlap) = continuation_overlap {
                rendered.push_str(&overlap);
            }
            rendered.push_str("\n## Source items (canonical Session order)\n");
            for (_, item) in items {
                rendered.push_str(&item);
            }
            if batch_index + 1 < batch_count {
                rendered.push_str("\n[retrieval_delta_continues_in_next_batch]\n");
            } else {
                rendered.push_str("\n[retrieval_delta_final_batch]\n");
            }
            debug_assert!(rendered.chars().count() <= RETRIEVAL_EXTRACTION_MAX_CHARS);
            rendered
        })
        .collect()
}

async fn collect_candidate_sessions(
    ctx: &AutoDreamContext,
    since: DateTime<Utc>,
) -> Vec<(SessionIndexEntry, Option<String>)> {
    let mut items = ctx.session_store.list_index_entries().await;
    items.retain(|entry| session_is_candidate(entry, since));
    items.sort_by_key(|e| Reverse(e.updated_at));

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
                .or_else(|| derive_sanitized_session_outline(&session)),
            _ => None,
        };
        out.push((entry, summary));
        if out.len() >= DREAM_MAX_SESSIONS {
            break;
        }
    }
    out
}

async fn resolve_session_project_id(
    ctx: &AutoDreamContext,
    session_id: &str,
) -> Option<bamboo_domain::ProjectId> {
    ctx.storage
        .load_session(session_id)
        .await
        .ok()
        .flatten()
        .and_then(|session| ProjectContextResolver::memory_read_identity_for_session(&session))
}

async fn collect_candidate_sessions_for_project(
    ctx: &AutoDreamContext,
    project_key: &str,
    since: DateTime<Utc>,
) -> Vec<(SessionIndexEntry, Option<String>)> {
    let mut out = Vec::new();
    for (entry, summary) in collect_candidate_sessions(ctx, since).await {
        let Some(project_id) = resolve_session_project_id(ctx, &entry.id).await else {
            continue;
        };
        if project_id.as_str() != project_key {
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
    for (entry, _) in sessions {
        let extraction_watermark = match memory.read_session_state(&entry.id).await {
            Ok(state) => state
                .last_extracted_at
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|timestamp| timestamp.with_timezone(&Utc)),
            Err(error) => {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "session_extraction_state_read_failed",
                    session_id = %entry.id,
                    "Could not read Jiandu session extraction state; keeping the session retryable: {error}"
                );
                None
            }
        };
        let session = match ctx.storage.load_session(&entry.id).await {
            Ok(Some(session)) => session,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "session_extraction_source_load_failed",
                    session_id = %entry.id,
                    "Could not load the canonical Session after reading its Jiandu extraction watermark; keeping the source retryable: {error}"
                );
                continue;
            }
        };
        let is_retrieval_window = session.conversation_summary.is_none()
            && session
                .compression_events
                .iter()
                .any(|event| event.kind == CompressionEventKind::RetrievalWindow);
        let retrieval_event_key = is_retrieval_window
            .then(|| first_retrieval_event_key(&session))
            .flatten();
        let retrieval_source_acknowledged = if is_retrieval_window {
            match retrieval_source_is_acknowledged(ctx, &session, extraction_watermark).await {
                Ok(acknowledged) => acknowledged,
                Err(error) => {
                    tracing::warn!(
                        target: DREAM_TRACING_TARGET,
                        event = "retrieval_source_state_read_failed",
                        session_id = %entry.id,
                        "Could not verify retrieval-complete source state; replaying the one-time transition: {error}"
                    );
                    false
                }
            }
        } else {
            false
        };
        if extraction_watermark.is_some_and(|watermark| watermark >= entry.updated_at)
            && (!is_retrieval_window || retrieval_source_acknowledged)
        {
            continue;
        }
        let summaries = session_extraction_sources(
            &session,
            extraction_watermark,
            retrieval_source_acknowledged,
        );
        let project_key = ProjectContextResolver::memory_read_identity_for_session(&session)
            .map(bamboo_domain::ProjectId::into_string);
        let topics = match memory.read_session_topics_with_content(&entry.id).await {
            Ok(topics) => sanitized_session_topics(topics),
            Err(error) => {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "session_extraction_topics_read_failed",
                    session_id = %entry.id,
                    "Could not load Jiandu Session topics; keeping the source retryable: {error}"
                );
                continue;
            }
        };
        if topics.is_empty()
            && summaries.iter().all(|summary| {
                summary
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            })
        {
            continue;
        }
        if is_retrieval_window {
            // Each bounded transcript batch receives its own candidate budget.
            // Session topics are a separate source unit so they cannot consume
            // the eight-candidate allowance for a transcript batch.
            for summary in summaries.into_iter().filter(|summary| {
                summary
                    .as_deref()
                    .is_some_and(|content| !content.trim().is_empty())
            }) {
                out.push(CandidateSessionContext {
                    session_id: entry.id.clone(),
                    project_key: project_key.clone(),
                    entry: entry.clone(),
                    summary,
                    topics: Vec::new(),
                    retrieval_event_key: retrieval_event_key.clone(),
                });
            }
            if !topics.is_empty() {
                out.push(CandidateSessionContext {
                    session_id: entry.id.clone(),
                    project_key,
                    entry,
                    summary: None,
                    topics,
                    retrieval_event_key,
                });
            }
        } else {
            out.push(CandidateSessionContext {
                session_id: entry.id.clone(),
                project_key,
                entry,
                summary: summaries.into_iter().next().flatten(),
                topics,
                retrieval_event_key: None,
            });
        }
    }
    out
}

fn sanitized_session_topics(topics: Vec<(String, String)>) -> Vec<(String, String)> {
    topics
        .into_iter()
        .take(EXTRACTION_MAX_TOPICS_PER_SESSION)
        .map(|(topic, content)| {
            // Check the complete pair before truncation so a credential that
            // crosses the bounded provider view cannot leak a prefix.
            let (topic, content) = sanitize_extraction_source_pair(&topic, &content);
            (topic, truncate_chars(&content, EXTRACTION_MAX_TOPIC_CHARS))
        })
        .collect()
}

#[cfg(test)]
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

/// Counts of records persisted from one extraction response: durable memory
/// candidates and ledger (commitment) candidates share a single LLM call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ExtractionWrites {
    memory: usize,
    ledger: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtractedCandidateBatch {
    memory: Vec<DurableExtractionCandidate>,
    ledger: Vec<LedgerExtractionCandidate>,
}

/// Retry state contains only parsed, privacy-checked candidates and hashed
/// source identity. Raw Session text, prompts, provider payloads, tool
/// arguments/results, paths, and pre-existing memory bodies are deliberately
/// absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtractionCheckpoint {
    version: u32,
    batch_id: String,
    session_key: String,
    source_updated_at: String,
    transaction_id: String,
    batch_index: usize,
    batch_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    topics_fingerprint: Option<String>,
    extracted: ExtractedCandidateBatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetrievalSourceState {
    version: u32,
    session_key: String,
    first_retrieval_event_key: String,
    source_updated_at: String,
}

#[derive(Debug)]
struct PreparedExtractionBatch {
    context_index: usize,
    extracted: ExtractedCandidateBatch,
}

#[derive(Debug)]
struct PendingExtractionBatch {
    context_index: usize,
    prompt: String,
    checkpoint_id: String,
    transaction_id: String,
    batch_index: usize,
    batch_count: usize,
    topics_fingerprint: String,
}

#[derive(Debug)]
struct StoredExtractionTransaction {
    source_updated_at: String,
    transaction_id: String,
    batches: Vec<ExtractionCheckpoint>,
}

#[derive(Debug, Clone)]
struct ReplayedExtractionSource {
    watermark: DateTime<Utc>,
    topics_fingerprint: Option<String>,
}

#[derive(Debug)]
struct RebuiltRetrievalContexts {
    contexts: Vec<CandidateSessionContext>,
}

fn extraction_topics_fingerprint(topics: &[(String, String)]) -> String {
    let mut topics = topics.to_vec();
    topics.sort();
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-session-topics-v1\0");
    for (topic, content) in topics {
        digest.update((topic.len() as u64).to_le_bytes());
        digest.update(topic.as_bytes());
        digest.update((content.len() as u64).to_le_bytes());
        digest.update(content.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn extraction_topics_fingerprints_by_session(
    sessions: &[CandidateSessionContext],
) -> HashMap<String, String> {
    let mut topics_by_session: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for session in sessions {
        topics_by_session
            .entry(session.session_id.clone())
            .or_default()
            .extend(session.topics.iter().cloned());
    }
    topics_by_session
        .into_iter()
        .map(|(session_id, topics)| (session_id, extraction_topics_fingerprint(&topics)))
        .collect()
}

fn extraction_prompt(session: &CandidateSessionContext) -> String {
    let provider_session_id = provider_session_alias(0);
    let provider_project_key = session
        .project_key
        .as_ref()
        .map(|_| provider_project_alias(0));
    build_extraction_prompt(&[sanitized_extraction_candidate_info(
        session,
        provider_session_id,
        provider_project_key,
    )])
}

fn extraction_checkpoint_session_key(session_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-extraction-session-v1\0");
    digest.update(session_id.as_bytes());
    hex::encode(digest.finalize())
}

fn first_retrieval_event_key(session: &Session) -> Option<String> {
    let event = session
        .compression_events
        .iter()
        .filter(|event| event.kind == CompressionEventKind::RetrievalWindow)
        .min_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        })?;
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-first-retrieval-event-v1\0");
    digest.update(event.id.as_bytes());
    digest.update(b"\0");
    digest.update(event.created_at.to_rfc3339().as_bytes());
    Some(hex::encode(digest.finalize()))
}

fn extraction_checkpoint_id(
    model: &str,
    session_id: &str,
    source_updated_at: &str,
    prompt: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-extraction-checkpoint-v3\0");
    digest.update(model.as_bytes());
    digest.update(b"\0");
    digest.update(extraction_checkpoint_session_key(session_id).as_bytes());
    digest.update(b"\0");
    digest.update(source_updated_at.as_bytes());
    digest.update(b"\0");
    digest.update(prompt.as_bytes());
    hex::encode(digest.finalize())
}

fn extraction_transaction_id(
    session_id: &str,
    source_updated_at: &str,
    checkpoint_ids: &[String],
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-extraction-transaction-v3\0");
    digest.update(extraction_checkpoint_session_key(session_id).as_bytes());
    digest.update(b"\0");
    digest.update(source_updated_at.as_bytes());
    for checkpoint_id in checkpoint_ids {
        digest.update(b"\0");
        digest.update(checkpoint_id.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn build_pending_extraction_batches(
    model: &str,
    sessions: &[CandidateSessionContext],
) -> Vec<PendingExtractionBatch> {
    let topics_fingerprints = extraction_topics_fingerprints_by_session(sessions);
    sessions
        .iter()
        .enumerate()
        .map(|(context_index, session)| {
            let prompt = extraction_prompt(session);
            let source_updated_at = session.entry.updated_at.to_rfc3339();
            PendingExtractionBatch {
                context_index,
                checkpoint_id: extraction_checkpoint_id(
                    model,
                    &session.session_id,
                    &source_updated_at,
                    &prompt,
                ),
                prompt,
                transaction_id: String::new(),
                batch_index: 0,
                batch_count: 0,
                topics_fingerprint: topics_fingerprints
                    .get(session.session_id.as_str())
                    .cloned()
                    .unwrap_or_else(|| extraction_topics_fingerprint(&[])),
            }
        })
        .collect()
}

fn assign_pending_extraction_transactions(
    sessions: &[CandidateSessionContext],
    pending_batches: &mut [PendingExtractionBatch],
) -> HashMap<String, Vec<usize>> {
    let mut indexes_by_session: HashMap<String, Vec<usize>> = HashMap::new();
    for (pending_index, pending) in pending_batches.iter().enumerate() {
        indexes_by_session
            .entry(sessions[pending.context_index].session_id.clone())
            .or_default()
            .push(pending_index);
    }
    for (session_id, pending_indexes) in &indexes_by_session {
        let checkpoint_ids = pending_indexes
            .iter()
            .map(|index| pending_batches[*index].checkpoint_id.clone())
            .collect::<Vec<_>>();
        let source_updated_at = sessions[pending_batches[pending_indexes[0]].context_index]
            .entry
            .updated_at
            .to_rfc3339();
        let transaction_id =
            extraction_transaction_id(session_id, &source_updated_at, &checkpoint_ids);
        let batch_count = pending_indexes.len();
        for (batch_index, pending_index) in pending_indexes.iter().enumerate() {
            let pending = &mut pending_batches[*pending_index];
            pending.transaction_id.clone_from(&transaction_id);
            pending.batch_index = batch_index;
            pending.batch_count = batch_count;
        }
    }
    indexes_by_session
}

fn checkpoint_matches_pending_batch(
    checkpoint: &ExtractionCheckpoint,
    session_id: &str,
    source_updated_at: &str,
    pending: &PendingExtractionBatch,
) -> bool {
    checkpoint.session_key == extraction_checkpoint_session_key(session_id)
        && checkpoint.source_updated_at == source_updated_at
        && checkpoint.transaction_id == pending.transaction_id
        && checkpoint.batch_id == pending.checkpoint_id
        && checkpoint.batch_index == pending.batch_index
        && checkpoint.batch_count == pending.batch_count
        && checkpoint
            .topics_fingerprint
            .as_deref()
            .is_none_or(|fingerprint| fingerprint == pending.topics_fingerprint)
}

fn extraction_checkpoint_session_dir(ctx: &AutoDreamContext, session_id: &str) -> PathBuf {
    ctx.session_store
        .bamboo_home_dir()
        .join(EXTRACTION_CHECKPOINT_DIR)
        .join(extraction_checkpoint_session_key(session_id))
}

fn retrieval_source_state_path(
    ctx: &AutoDreamContext,
    session_id: &str,
    first_retrieval_event_key: &str,
) -> PathBuf {
    extraction_checkpoint_session_dir(ctx, session_id).join(format!(
        "retrieval-source-v{RETRIEVAL_SOURCE_STATE_VERSION}-{first_retrieval_event_key}.json"
    ))
}

async fn retrieval_source_is_acknowledged(
    ctx: &AutoDreamContext,
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
) -> Result<bool, String> {
    let Some(event_key) = first_retrieval_event_key(session) else {
        return Ok(false);
    };
    let path = retrieval_source_state_path(ctx, &session.id, &event_key);
    let raw = match tokio::fs::read(&path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(format!(
                "failed to read AutoDream retrieval source state: {error}"
            ));
        }
    };
    let state = serde_json::from_slice::<RetrievalSourceState>(&raw)
        .map_err(|error| format!("failed to parse AutoDream retrieval source state: {error}"))?;
    let source_updated_at = DateTime::parse_from_rfc3339(&state.source_updated_at)
        .map_err(|error| format!("invalid AutoDream retrieval source timestamp: {error}"))?
        .with_timezone(&Utc);
    if state.version != RETRIEVAL_SOURCE_STATE_VERSION
        || state.session_key != extraction_checkpoint_session_key(&session.id)
        || state.first_retrieval_event_key != event_key
        || source_updated_at > session.updated_at
    {
        return Err("AutoDream retrieval source state identity mismatch".to_string());
    }
    Ok(extraction_watermark.is_some_and(|watermark| watermark >= source_updated_at))
}

async fn write_retrieval_source_state(
    ctx: &AutoDreamContext,
    session_id: &str,
    first_retrieval_event_key: &str,
    source_updated_at: &str,
) -> Result<(), String> {
    let state = RetrievalSourceState {
        version: RETRIEVAL_SOURCE_STATE_VERSION,
        session_key: extraction_checkpoint_session_key(session_id),
        first_retrieval_event_key: first_retrieval_event_key.to_string(),
        source_updated_at: source_updated_at.to_string(),
    };
    DateTime::parse_from_rfc3339(source_updated_at)
        .map_err(|error| format!("invalid retrieval source watermark: {error}"))?;
    let bytes = serde_json::to_vec_pretty(&state)
        .map_err(|error| format!("failed to serialize retrieval source state: {error}"))?;
    let path = retrieval_source_state_path(ctx, session_id, first_retrieval_event_key);
    let parent = path
        .parent()
        .ok_or_else(|| "AutoDream retrieval source state has no parent directory".to_string())?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| format!("failed to create retrieval source state directory: {error}"))?;
    let temporary_path = parent.join(format!(".retrieval-source.{}.tmp", uuid::Uuid::new_v4()));
    let write_result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::hard_link(&temporary_path, &path).await
    }
    .await;
    let _ = tokio::fs::remove_file(&temporary_path).await;
    match write_result {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = tokio::fs::read(&path)
                .await
                .map_err(|error| format!("failed to reread retrieval source state: {error}"))?;
            let existing =
                serde_json::from_slice::<RetrievalSourceState>(&existing).map_err(|error| {
                    format!("failed to parse existing retrieval source state: {error}")
                })?;
            if existing.version != state.version
                || existing.session_key != state.session_key
                || existing.first_retrieval_event_key != state.first_retrieval_event_key
                || DateTime::parse_from_rfc3339(&existing.source_updated_at).is_err()
            {
                return Err("existing AutoDream retrieval source state mismatch".to_string());
            }
            Ok(())
        }
        Err(error) => Err(format!(
            "failed to persist AutoDream retrieval source state: {error}"
        )),
    }
}

fn extraction_checkpoint_path(
    ctx: &AutoDreamContext,
    session_id: &str,
    checkpoint_id: &str,
) -> PathBuf {
    extraction_checkpoint_session_dir(ctx, session_id).join(format!("{checkpoint_id}.json"))
}

fn validate_extracted_candidate_batch(extracted: &ExtractedCandidateBatch) -> Result<(), String> {
    if extracted.memory.len() > EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH
        || extracted.ledger.len() > EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH
    {
        return Err(format!(
            "AutoDream extraction checkpoint exceeds the {} candidate safety cap",
            EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH
        ));
    }
    if !extracted
        .memory
        .iter()
        .all(durable_candidate_is_secret_safe)
        || !extracted.ledger.iter().all(ledger_candidate_is_secret_safe)
    {
        return Err(
            "AutoDream extraction checkpoint contains a candidate that failed the privacy boundary"
                .to_string(),
        );
    }
    Ok(())
}

async fn read_extraction_checkpoint(
    path: &Path,
    checkpoint_id: &str,
) -> Result<Option<ExtractionCheckpoint>, String> {
    let raw = match tokio::fs::read(path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to read AutoDream extraction checkpoint: {error}"
            ));
        }
    };
    let checkpoint: ExtractionCheckpoint = serde_json::from_slice(&raw)
        .map_err(|error| format!("failed to parse AutoDream extraction checkpoint: {error}"))?;
    if checkpoint.version != EXTRACTION_CHECKPOINT_VERSION
        || checkpoint.batch_id != checkpoint_id
        || checkpoint.session_key.len() != 64
        || !checkpoint
            .session_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || checkpoint.transaction_id.len() != 64
        || !checkpoint
            .transaction_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || checkpoint.batch_count == 0
        || checkpoint.batch_index >= checkpoint.batch_count
        || DateTime::parse_from_rfc3339(&checkpoint.source_updated_at).is_err()
    {
        return Err("AutoDream extraction checkpoint identity mismatch".to_string());
    }
    validate_extracted_candidate_batch(&checkpoint.extracted)?;
    Ok(Some(checkpoint))
}

async fn write_extraction_checkpoint(
    path: &Path,
    checkpoint: &ExtractionCheckpoint,
) -> Result<bool, String> {
    validate_extracted_candidate_batch(&checkpoint.extracted)?;
    let bytes = serde_json::to_vec_pretty(checkpoint)
        .map_err(|error| format!("failed to serialize AutoDream extraction checkpoint: {error}"))?;
    let parent = path
        .parent()
        .ok_or_else(|| "AutoDream extraction checkpoint has no parent directory".to_string())?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| format!("failed to create AutoDream checkpoint directory: {error}"))?;
    let temporary_path = parent.join(format!(
        ".{}.{}.tmp",
        checkpoint.batch_id,
        uuid::Uuid::new_v4()
    ));
    let write_result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::hard_link(&temporary_path, path).await
    }
    .await;
    let _ = tokio::fs::remove_file(&temporary_path).await;
    match write_result {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(format!(
            "failed to persist AutoDream extraction checkpoint: {error}"
        )),
    }
}

async fn remove_extraction_checkpoint(
    ctx: &AutoDreamContext,
    session_id: &str,
    checkpoint_id: &str,
) {
    let path = extraction_checkpoint_path(ctx, session_id, checkpoint_id);
    if let Err(error) = tokio::fs::remove_file(path).await {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                target: DREAM_TRACING_TARGET,
                event = "extraction_checkpoint_cleanup_failed",
                checkpoint_id,
                "Could not remove an acknowledged AutoDream extraction checkpoint: {error}"
            );
        }
    }
}

async fn load_extraction_checkpoint_transactions(
    ctx: &AutoDreamContext,
    session_id: &str,
) -> Result<Vec<StoredExtractionTransaction>, String> {
    let directory = extraction_checkpoint_session_dir(ctx, session_id);
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "failed to inspect AutoDream extraction checkpoints: {error}"
            ));
        }
    };
    let expected_session_key = extraction_checkpoint_session_key(session_id);
    let mut grouped: HashMap<String, Vec<ExtractionCheckpoint>> = HashMap::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| format!("failed to enumerate AutoDream extraction checkpoints: {error}"))?
    {
        let path = entry.path();
        let Some(checkpoint_id) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".json"))
        else {
            continue;
        };
        if checkpoint_id.len() != 64 || !checkpoint_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            continue;
        }
        let checkpoint = read_extraction_checkpoint(&path, checkpoint_id)
            .await?
            .ok_or_else(|| {
                "AutoDream extraction checkpoint disappeared during inspection".to_string()
            })?;
        if checkpoint.session_key != expected_session_key {
            return Err("AutoDream extraction checkpoint Session identity mismatch".to_string());
        }
        grouped
            .entry(checkpoint.transaction_id.clone())
            .or_default()
            .push(checkpoint);
    }

    let mut complete = Vec::new();
    for (transaction_id, mut batches) in grouped {
        batches.sort_by_key(|batch| batch.batch_index);
        let first = batches
            .first()
            .expect("checkpoint group constructed from at least one entry");
        if batches.iter().any(|batch| {
            batch.transaction_id != transaction_id
                || batch.source_updated_at != first.source_updated_at
                || batch.batch_count != first.batch_count
                || batch.topics_fingerprint != first.topics_fingerprint
        }) {
            return Err("AutoDream extraction transaction metadata mismatch".to_string());
        }
        if batches.len() != first.batch_count
            || !batches
                .iter()
                .enumerate()
                .all(|(index, batch)| batch.batch_index == index)
        {
            continue;
        }
        let ordered_ids = batches
            .iter()
            .map(|batch| batch.batch_id.clone())
            .collect::<Vec<_>>();
        if extraction_transaction_id(session_id, &first.source_updated_at, &ordered_ids)
            != transaction_id
        {
            return Err("AutoDream extraction transaction identity mismatch".to_string());
        }
        complete.push(StoredExtractionTransaction {
            source_updated_at: first.source_updated_at.clone(),
            transaction_id,
            batches,
        });
    }
    complete.sort_by(|left, right| {
        left.source_updated_at
            .cmp(&right.source_updated_at)
            .then_with(|| left.transaction_id.cmp(&right.transaction_id))
    });
    Ok(complete)
}

async fn remove_superseded_extraction_checkpoints(
    ctx: &AutoDreamContext,
    session_id: &str,
    retained_checkpoint_ids: &HashSet<String>,
) -> Result<(), String> {
    let directory = extraction_checkpoint_session_dir(ctx, session_id);
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "failed to inspect AutoDream extraction checkpoints: {error}"
            ));
        }
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| format!("failed to enumerate AutoDream extraction checkpoints: {error}"))?
    {
        let path = entry.path();
        let Some(checkpoint_id) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".json"))
        else {
            continue;
        };
        if checkpoint_id.len() != 64
            || !checkpoint_id.bytes().all(|byte| byte.is_ascii_hexdigit())
            || retained_checkpoint_ids.contains(checkpoint_id)
        {
            continue;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to remove superseded AutoDream extraction checkpoint: {error}"
                ));
            }
        }
    }
    Ok(())
}

async fn rebuild_retrieval_contexts_after_checkpoint_replay(
    ctx: &AutoDreamContext,
    template: &CandidateSessionContext,
    acknowledged_watermark: DateTime<Utc>,
    acknowledged_topics_fingerprint: Option<&str>,
) -> Result<RebuiltRetrievalContexts, String> {
    let session = ctx
        .storage
        .load_session(&template.session_id)
        .await
        .map_err(|error| {
            format!(
                "failed to reload retrieval source after checkpoint replay for {}: {error}",
                template.session_id
            )
        })?
        .ok_or_else(|| {
            format!(
                "retrieval source disappeared after checkpoint replay for {}",
                template.session_id
            )
        })?;
    if session.conversation_summary.is_some()
        || !session
            .compression_events
            .iter()
            .any(|event| event.kind == CompressionEventKind::RetrievalWindow)
    {
        return Err(format!(
            "retrieval source mode changed during checkpoint replay for {}",
            template.session_id
        ));
    }
    let retrieval_event_key = first_retrieval_event_key(&session)
        .ok_or_else(|| "retrieval source lost its first boundary identity".to_string())?;
    let project_key = ProjectContextResolver::memory_read_identity_for_session(&session)
        .map(bamboo_domain::ProjectId::into_string);
    let topics = sanitized_session_topics(
        ctx.memory
            .read_session_topics_with_content(&template.session_id)
            .await
            .map_err(|error| {
                format!(
                    "failed to reload retrieval Session topics after checkpoint replay for {}: {error}",
                    template.session_id
                )
            })?,
    );
    let current_topics_fingerprint = extraction_topics_fingerprint(&topics);
    let topics_changed = acknowledged_topics_fingerprint
        .is_none_or(|fingerprint| fingerprint != current_topics_fingerprint);
    let mut entry = template.entry.clone();
    entry.title.clone_from(&session.title);
    entry.updated_at = session.updated_at;

    let mut contexts =
        build_retrieval_window_extraction_batches(&session, Some(acknowledged_watermark), true)
            .into_iter()
            .filter(|summary| !summary.trim().is_empty())
            .map(|summary| CandidateSessionContext {
                entry: entry.clone(),
                summary: Some(summary),
                session_id: template.session_id.clone(),
                project_key: project_key.clone(),
                topics: Vec::new(),
                retrieval_event_key: Some(retrieval_event_key.clone()),
            })
            .collect::<Vec<_>>();
    if topics_changed && !topics.is_empty() {
        contexts.push(CandidateSessionContext {
            entry,
            summary: None,
            session_id: template.session_id.clone(),
            project_key,
            topics,
            retrieval_event_key: Some(retrieval_event_key),
        });
    }
    Ok(RebuiltRetrievalContexts { contexts })
}

#[cfg(test)]
async fn extract_and_persist_durable_candidates(
    ctx: &AutoDreamContext,
    provider: &Arc<dyn LLMProvider>,
    memory: &MemoryStore,
    ledger: &LedgerStore,
    model: &str,
    sessions: &[CandidateSessionContext],
) -> Result<ExtractionWrites, String> {
    extract_and_persist_durable_candidates_with_project_resolver(
        ctx, provider, memory, ledger, model, sessions, None, false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn extract_and_persist_durable_candidates_with_project_resolver(
    ctx: &AutoDreamContext,
    provider: &Arc<dyn LLMProvider>,
    memory: &MemoryStore,
    ledger: &LedgerStore,
    model: &str,
    sessions: &[CandidateSessionContext],
    project_resolver: Option<&ProjectContextResolver>,
    current_store_is_project_scoped: bool,
) -> Result<ExtractionWrites, String> {
    if sessions.is_empty() {
        return Ok(ExtractionWrites::default());
    }

    let mut extraction_sessions = sessions.to_vec();
    let mut pending_batches = build_pending_extraction_batches(model, &extraction_sessions);
    let mut pending_indexes_by_session =
        assign_pending_extraction_transactions(&extraction_sessions, &mut pending_batches);
    let mut source_updated_at_by_session = extraction_sessions
        .iter()
        .map(|session| {
            (
                session.session_id.clone(),
                session.entry.updated_at.to_rfc3339(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut total_writes = ExtractionWrites::default();
    let mut completed_transactions = HashSet::new();
    let mut replayed_watermarks = HashMap::new();
    let mut replayed_sources_by_session = HashMap::new();

    // Replay complete candidate-only checkpoints before asking the provider for
    // a newer source. A later sink or watermark failure therefore never asks
    // the model to restate a prefix that was already extracted successfully.
    for (session_id, pending_indexes) in &pending_indexes_by_session {
        let retained = pending_indexes
            .iter()
            .map(|index| pending_batches[*index].checkpoint_id.clone())
            .collect::<HashSet<_>>();
        let transactions = load_extraction_checkpoint_transactions(ctx, session_id).await?;
        let context = extraction_sessions
            .iter()
            .find(|session| session.session_id == *session_id)
            .expect("pending extraction Session must have a context");
        let state = memory
            .read_session_state(session_id)
            .await
            .map_err(|error| {
                format!(
                    "failed to read extraction watermark before checkpoint replay for {session_id}: {error}"
                )
            })?;
        let mut acknowledged_watermark = state
            .last_extracted_at
            .as_deref()
            .map(DateTime::parse_from_rfc3339)
            .transpose()
            .map_err(|error| {
                format!(
                    "invalid extraction watermark before checkpoint replay for {session_id}: {error}"
                )
            })?
            .map(|timestamp| timestamp.with_timezone(&Utc));

        for transaction in transactions {
            let source_watermark = DateTime::parse_from_rfc3339(&transaction.source_updated_at)
                .expect("stored checkpoint timestamp was validated while loading")
                .with_timezone(&Utc);
            if source_watermark > context.entry.updated_at {
                return Err(format!(
                    "AutoDream extraction checkpoint watermark is newer than Session authority for {session_id}"
                ));
            }
            let checkpoint_ids = transaction
                .batches
                .iter()
                .map(|batch| batch.batch_id.clone())
                .collect::<Vec<_>>();
            let transaction_topics_fingerprint = transaction
                .batches
                .first()
                .and_then(|batch| batch.topics_fingerprint.clone());
            if acknowledged_watermark.is_none_or(|watermark| watermark < source_watermark) {
                for checkpoint in transaction.batches {
                    let writes = persist_durable_candidate_batch_with_project_resolver(
                        ctx,
                        memory,
                        ledger,
                        std::slice::from_ref(context),
                        checkpoint.extracted,
                        project_resolver,
                        current_store_is_project_scoped,
                    )
                    .await?;
                    total_writes.memory = total_writes.memory.saturating_add(writes.memory);
                    total_writes.ledger = total_writes.ledger.saturating_add(writes.ledger);
                }
                if let Some(event_key) = extraction_sessions
                    .iter()
                    .filter(|session| session.session_id.as_str() == session_id.as_str())
                    .find_map(|session| session.retrieval_event_key.as_deref())
                {
                    write_retrieval_source_state(
                        ctx,
                        session_id,
                        event_key,
                        &transaction.source_updated_at,
                    )
                    .await?;
                }
                memory
                    .mark_session_extracted(session_id, &transaction.source_updated_at)
                    .await
                    .map_err(|error| {
                        format!(
                            "failed to acknowledge replayed extraction transaction for {session_id}: {error}"
                        )
                    })?;
                acknowledged_watermark = Some(source_watermark);
                replayed_watermarks.insert(session_id.clone(), source_watermark);
                replayed_sources_by_session.insert(
                    session_id.clone(),
                    ReplayedExtractionSource {
                        watermark: source_watermark,
                        topics_fingerprint: transaction_topics_fingerprint,
                    },
                );
            }
            completed_transactions.insert((session_id.clone(), transaction.transaction_id));
            for checkpoint_id in checkpoint_ids {
                remove_extraction_checkpoint(ctx, session_id, &checkpoint_id).await;
            }
        }
        remove_superseded_extraction_checkpoints(ctx, session_id, &retained).await?;
    }

    // The current contexts were collected before an older checkpoint replay
    // advanced Jiandu's watermark. Rebuild retrieval deltas against that exact
    // acknowledged boundary so intervening turns remain eligible without
    // asking the model to restate the completed prefix.
    let retrieval_sessions_to_rebuild = replayed_sources_by_session
        .keys()
        .filter(|session_id| {
            extraction_sessions.iter().any(|session| {
                session.session_id.as_str() == session_id.as_str()
                    && session.retrieval_event_key.is_some()
            })
        })
        .cloned()
        .collect::<HashSet<_>>();
    if !retrieval_sessions_to_rebuild.is_empty() {
        let templates = retrieval_sessions_to_rebuild
            .iter()
            .map(|session_id| {
                extraction_sessions
                    .iter()
                    .find(|session| session.session_id == *session_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "missing retrieval context after checkpoint replay for {session_id}"
                        )
                    })
                    .map(|template| (session_id.clone(), template))
            })
            .collect::<Result<Vec<_>, String>>()?;
        extraction_sessions
            .retain(|session| !retrieval_sessions_to_rebuild.contains(session.session_id.as_str()));
        for (session_id, template) in templates {
            let replayed = replayed_sources_by_session
                .get(&session_id)
                .expect("rebuild Session came from replayed source map");
            let rebuilt = rebuild_retrieval_contexts_after_checkpoint_replay(
                ctx,
                &template,
                replayed.watermark,
                replayed.topics_fingerprint.as_deref(),
            )
            .await?;
            extraction_sessions.extend(rebuilt.contexts);
        }
        pending_batches = build_pending_extraction_batches(model, &extraction_sessions);
        pending_indexes_by_session =
            assign_pending_extraction_transactions(&extraction_sessions, &mut pending_batches);
        source_updated_at_by_session = extraction_sessions
            .iter()
            .map(|session| {
                (
                    session.session_id.clone(),
                    session.entry.updated_at.to_rfc3339(),
                )
            })
            .collect();
        for (session_id, pending_indexes) in &pending_indexes_by_session {
            let retained = pending_indexes
                .iter()
                .map(|index| pending_batches[*index].checkpoint_id.clone())
                .collect::<HashSet<_>>();
            remove_superseded_extraction_checkpoints(ctx, session_id, &retained).await?;
        }
    }

    pending_batches.retain(|pending| {
        let session = &extraction_sessions[pending.context_index];
        !completed_transactions
            .contains(&(session.session_id.clone(), pending.transaction_id.clone()))
            && replayed_watermarks
                .get(&session.session_id)
                .is_none_or(|watermark| session.entry.updated_at >= *watermark)
    });
    if pending_batches.is_empty() {
        return Ok(total_writes);
    }

    let mut prepared_batches = Vec::with_capacity(pending_batches.len());
    let mut checkpoint_ids_by_session: HashMap<String, Vec<String>> = HashMap::new();
    for pending in &pending_batches {
        checkpoint_ids_by_session
            .entry(
                extraction_sessions[pending.context_index]
                    .session_id
                    .clone(),
            )
            .or_default()
            .push(pending.checkpoint_id.clone());
    }

    // Complete and durably checkpoint every provider batch before either sink
    // starts. A later provider failure therefore produces zero sink writes.
    for pending in pending_batches {
        let session = &extraction_sessions[pending.context_index];
        let source_updated_at = source_updated_at_by_session
            .get(&session.session_id)
            .expect("every pending Session has a source watermark");
        let checkpoint_path =
            extraction_checkpoint_path(ctx, &session.session_id, &pending.checkpoint_id);
        let extracted = match read_extraction_checkpoint(&checkpoint_path, &pending.checkpoint_id)
            .await?
        {
            Some(checkpoint) => {
                if !checkpoint_matches_pending_batch(
                    &checkpoint,
                    &session.session_id,
                    source_updated_at,
                    &pending,
                ) {
                    return Err("AutoDream pending checkpoint metadata mismatch".to_string());
                }
                checkpoint.extracted
            }
            None => {
                let extracted = extract_durable_candidate_batch(
                    provider,
                    model,
                    pending.prompt.clone(),
                    &session.session_id,
                )
                .await?;
                let checkpoint = ExtractionCheckpoint {
                    version: EXTRACTION_CHECKPOINT_VERSION,
                    batch_id: pending.checkpoint_id.clone(),
                    session_key: extraction_checkpoint_session_key(&session.session_id),
                    source_updated_at: source_updated_at.clone(),
                    transaction_id: pending.transaction_id.clone(),
                    batch_index: pending.batch_index,
                    batch_count: pending.batch_count,
                    topics_fingerprint: Some(pending.topics_fingerprint.clone()),
                    extracted: extracted.clone(),
                };
                if write_extraction_checkpoint(&checkpoint_path, &checkpoint).await? {
                    extracted
                } else {
                    let checkpoint =
                        read_extraction_checkpoint(&checkpoint_path, &pending.checkpoint_id)
                            .await?
                            .ok_or_else(|| {
                                "concurrent AutoDream checkpoint disappeared before reuse"
                                    .to_string()
                            })?;
                    if !checkpoint_matches_pending_batch(
                        &checkpoint,
                        &session.session_id,
                        source_updated_at,
                        &pending,
                    ) {
                        return Err("concurrent AutoDream checkpoint metadata mismatch".to_string());
                    }
                    checkpoint.extracted
                }
            }
        };
        prepared_batches.push(PreparedExtractionBatch {
            context_index: pending.context_index,
            extracted,
        });
    }

    for batch in prepared_batches {
        let writes = persist_durable_candidate_batch_with_project_resolver(
            ctx,
            memory,
            ledger,
            std::slice::from_ref(&extraction_sessions[batch.context_index]),
            batch.extracted,
            project_resolver,
            current_store_is_project_scoped,
        )
        .await?;
        total_writes.memory = total_writes.memory.saturating_add(writes.memory);
        total_writes.ledger = total_writes.ledger.saturating_add(writes.ledger);
    }

    for (session_id, checkpoint_ids) in checkpoint_ids_by_session {
        let source_updated_at = source_updated_at_by_session
            .get(&session_id)
            .expect("every checkpointed Session has a source watermark");
        if let Some(event_key) = extraction_sessions
            .iter()
            .filter(|session| session.session_id.as_str() == session_id.as_str())
            .find_map(|session| session.retrieval_event_key.as_deref())
        {
            write_retrieval_source_state(ctx, &session_id, event_key, source_updated_at).await?;
        }
        memory
            .mark_session_extracted(&session_id, source_updated_at)
            .await
            .map_err(|error| {
                format!("failed to update session extraction state for {session_id}: {error}")
            })?;
        for checkpoint_id in checkpoint_ids {
            remove_extraction_checkpoint(ctx, &session_id, &checkpoint_id).await;
        }
    }

    Ok(total_writes)
}

async fn extract_durable_candidate_batch(
    provider: &Arc<dyn LLMProvider>,
    model: &str,
    prompt: String,
    session_id: &str,
) -> Result<ExtractedCandidateBatch, String> {
    let base_prompt = prompt;
    let mut request_prompt = base_prompt.clone();
    let mut memory = Vec::new();
    let mut ledger = Vec::new();
    let mut memory_fingerprints = HashSet::new();
    let mut ledger_fingerprints = HashSet::new();
    let provider_aliases = HashMap::from([(provider_session_alias(0), session_id.to_string())]);

    for page_index in 0..EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH {
        let raw = collect_stream_text(provider.clone(), model, request_prompt).await?;
        let mut page_memory = parse_extraction_candidates(&raw)?;
        let source_exhausted = extraction_page_source_exhausted(&raw)?;
        page_memory.retain_mut(|candidate| {
            restore_provider_session_alias(&mut candidate.session_id, &provider_aliases)
        });
        let mut page_ledger = parse_ledger_candidates(&raw);
        page_ledger.retain_mut(|candidate| {
            restore_provider_session_alias(&mut candidate.session_id, &provider_aliases)
        });
        let memory_count_before = memory.len();
        let ledger_count_before = ledger.len();

        for candidate in page_memory
            .into_iter()
            .filter(durable_candidate_is_secret_safe)
        {
            let fingerprint = serde_json::to_string(&candidate)
                .map_err(|error| format!("failed to fingerprint memory candidate: {error}"))?;
            if memory_fingerprints.insert(fingerprint) {
                memory.push(candidate);
            }
        }
        for candidate in page_ledger
            .into_iter()
            .filter(ledger_candidate_is_secret_safe)
        {
            let fingerprint = serde_json::to_string(&candidate)
                .map_err(|error| format!("failed to fingerprint Ledger candidate: {error}"))?;
            if ledger_fingerprints.insert(fingerprint) {
                ledger.push(candidate);
            }
        }

        if source_exhausted {
            return Ok(ExtractedCandidateBatch { memory, ledger });
        }
        if memory.len() == memory_count_before && ledger.len() == ledger_count_before {
            return Err(
                "AutoDream extraction declared remaining candidates but its continuation page made no safe, deduplicated progress"
                    .to_string(),
            );
        }
        request_prompt =
            extraction_continuation_prompt(&base_prompt, page_index + 2, &memory, &ledger)?;
    }

    Err(format!(
        "AutoDream extraction did not exhaust its source within {} pages; source watermark was not acknowledged",
        EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH
    ))
}

fn extraction_page_source_exhausted(raw: &str) -> Result<bool, String> {
    let value =
        serde_json::from_str::<serde_json::Value>(bamboo_memory::auto_dream::strip_json_fence(raw))
            .map_err(|error| format!("failed to parse extraction page status: {error}"))?;
    let candidate_count = |field: &str| {
        value
            .get(field)
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len)
    };
    let memory_candidate_count = candidate_count("candidates");
    let ledger_candidate_count = candidate_count("ledger_candidates");
    for (label, count) in [
        ("durable-memory", memory_candidate_count),
        ("Ledger", ledger_candidate_count),
    ] {
        if count > EXTRACTION_MAX_CANDIDATES {
            return Err(format!(
                "AutoDream extraction page returned {count} {label} candidates; maximum is {}",
                EXTRACTION_MAX_CANDIDATES
            ));
        }
    }
    let page_is_saturated = memory_candidate_count == EXTRACTION_MAX_CANDIDATES
        || ledger_candidate_count == EXTRACTION_MAX_CANDIDATES;
    match value.get("source_exhausted") {
        Some(serde_json::Value::Bool(exhausted)) => Ok(*exhausted),
        Some(_) => Err("AutoDream extraction source_exhausted must be a boolean".to_string()),
        None if !page_is_saturated => Ok(true),
        None => Err(
            "AutoDream extraction saturated the eight-candidate page without source_exhausted; source watermark was not acknowledged"
                .to_string(),
        ),
    }
}

fn extraction_continuation_prompt(
    base_prompt: &str,
    page_number: usize,
    memory: &[DurableExtractionCandidate],
    ledger: &[LedgerExtractionCandidate],
) -> Result<String, String> {
    let already_returned = serde_json::to_string(&serde_json::json!({
        "candidates": memory,
        "ledger_candidates": ledger,
    }))
    .map_err(|error| format!("failed to serialize extraction continuation state: {error}"))?;
    Ok(format!(
        "{base_prompt}\n\n## Exhaustive continuation page {page_number}\n\
The preceding response declared source_exhausted=false. Re-examine the same source, skip every candidate in already_returned, and return the next page only. Do not acknowledge exhaustion until every remaining durable-memory candidate has been emitted.\n\
- already_returned: {already_returned}\n"
    ))
}

#[allow(clippy::too_many_arguments)]
async fn persist_durable_candidate_batch_with_project_resolver(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    ledger: &LedgerStore,
    sessions: &[CandidateSessionContext],
    extracted: ExtractedCandidateBatch,
    project_resolver: Option<&ProjectContextResolver>,
    current_store_is_project_scoped: bool,
) -> Result<ExtractionWrites, String> {
    validate_extracted_candidate_batch(&extracted)?;
    let ExtractedCandidateBatch {
        memory: candidates,
        ledger: ledger_candidates,
    } = extracted;

    let mut session_project_keys = HashMap::new();
    for session in sessions {
        session_project_keys.insert(session.session_id.clone(), session.project_key.clone());
    }
    let ledger_candidates = ledger_candidates
        .into_iter()
        .filter(|candidate| {
            candidate
                .session_id
                .as_deref()
                .map(str::trim)
                .filter(|session_id| !session_id.is_empty())
                .is_none_or(|session_id| session_project_keys.contains_key(session_id))
        })
        .collect();

    let mut writes = 0usize;
    type ExtractionFingerprint = (DurableMemoryType, String, String, String);
    let mut existing_by_scope: HashMap<
        (MemoryScope, Option<String>),
        HashSet<ExtractionFingerprint>,
    > = HashMap::new();
    for candidate in candidates {
        let Some(memory_type) = parse_candidate_type(&candidate.kind) else {
            continue;
        };
        let title = candidate.title.trim();
        let content = candidate.content.trim();
        if title.is_empty() || content.is_empty() {
            continue;
        }
        if title.chars().count() > MAX_MEMORY_TITLE_LEN {
            tracing::warn!(
                target: DREAM_TRACING_TARGET,
                event = "memory_candidate_skipped",
                reason = "title_too_long",
                max_chars = MAX_MEMORY_TITLE_LEN,
                "Skipping an invalid AutoDream extraction candidate"
            );
            continue;
        }
        let Some(session_id) = candidate
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if !session_project_keys.contains_key(session_id) {
            tracing::warn!(
                target: DREAM_TRACING_TARGET,
                event = "memory_candidate_skipped",
                reason = "unknown_source_session",
                "Skipping an AutoDream candidate whose source was not in the extraction input"
            );
            continue;
        }
        if project_resolver.is_some() {
            let source_session = ctx
                .storage
                .load_session(session_id)
                .await
                .map_err(|error| {
                    format!("failed to load durable-memory source session '{session_id}': {error}")
                })?;
            if source_session.as_ref().is_some_and(|session| {
                matches!(
                    ProjectContextResolver::session_project_identity(session),
                    crate::project_context::SessionProjectIdentity::Invalid { .. }
                )
            }) {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "memory_candidate_skipped",
                    session_id,
                    reason = "invalid_project_identity",
                    "Skipping AutoDream extraction from a session with malformed Project identity"
                );
                continue;
            }
        }
        let project_key = session_project_keys
            .get(session_id)
            .and_then(|value| value.as_deref())
            .map(ToString::to_string);
        let scope = parse_candidate_scope(&candidate, project_key.as_deref());
        let mut write_memory = memory.clone();
        let mut write_project_key = project_key;
        if scope == MemoryScope::Project && !current_store_is_project_scoped {
            let Some(resolver) = project_resolver else {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "project_candidate_skipped",
                    session_id,
                    reason = "project_resolver_unavailable",
                    "Skipping Project memory extraction because stable Project authority is unavailable"
                );
                continue;
            };
            let session = ctx
                .storage
                .load_session(session_id)
                .await
                .map_err(|error| {
                    format!("failed to load Project memory source session '{session_id}': {error}")
                })?
                .ok_or_else(|| {
                    format!(
                        "Project memory extraction source session '{session_id}' no longer exists"
                    )
                })?;
            let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
            let resolved = resolver
                .resolve_memory_read_scope(&session, workspace.as_deref())
                .await
                .map_err(|error| {
                    format!(
                        "failed to resolve Project memory scope for session '{session_id}': {error}"
                    )
                })?;
            let Some(project_id) = resolved else {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "project_candidate_skipped",
                    session_id = session_id,
                    reason = "session_unassigned",
                    "Skipping Project memory extraction for an unassigned session"
                );
                continue;
            };
            write_project_key = Some(project_id.to_string());
            write_memory = memory.for_project(&project_id);
        }
        let tags = candidate.tags;
        let _ = &candidate.confidence;
        let scope_key = (scope, write_project_key.clone());
        if !existing_by_scope.contains_key(&scope_key) {
            let existing = write_memory
                .list_memory_documents(scope, write_project_key.as_deref())
                .await
                .map_err(|error| {
                    format!("failed to inspect durable extraction retry state: {error}")
                })?;
            let fingerprints = existing
                .into_iter()
                .filter(|document| document.frontmatter.status == DurableMemoryStatus::Active)
                .filter_map(|document| {
                    let source_session_id = document
                        .frontmatter
                        .sources
                        .iter()
                        .find(|source| source.kind == "session")?
                        .id
                        .clone();
                    Some((
                        document.frontmatter.r#type,
                        document.frontmatter.title.trim().to_string(),
                        document.body.trim().to_string(),
                        source_session_id,
                    ))
                })
                .collect();
            existing_by_scope.insert(scope_key.clone(), fingerprints);
        }
        let fingerprint = (
            memory_type,
            title.to_string(),
            content.to_string(),
            session_id.to_string(),
        );
        if existing_by_scope
            .get(&scope_key)
            .is_some_and(|existing| existing.contains(&fingerprint))
        {
            continue;
        }
        write_memory
            .write_memory(
                scope,
                write_project_key.as_deref(),
                memory_type,
                title,
                content,
                &tags,
                Some(session_id),
                "background-fast-model",
                false,
                None,
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to persist durable extraction candidate '{}': {error}",
                    title
                )
            })?;
        writes += 1;
        existing_by_scope
            .get_mut(&scope_key)
            .expect("scope retry state was initialized before the write")
            .insert(fingerprint);
    }

    let ledger_writes = persist_ledger_candidates(ledger, ledger_candidates).await?;

    Ok(ExtractionWrites {
        memory: writes,
        ledger: ledger_writes,
    })
}

fn normalized_ledger_title(title: &str) -> String {
    title.trim().to_lowercase()
}

fn parse_candidate_timestamp(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .map(str::trim)
        .filter(|raw| !raw.is_empty())
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

/// Persist extractor-proposed ledger candidates as `suggested` Global records.
///
/// Rules (Phase 6 of the personal-assistant ledger design):
/// - only `high`/`medium` confidence candidates are written; `low` (or
///   missing) confidence is skipped;
/// - empty or over-long titles are skipped;
/// - a candidate whose normalized (case-insensitive, trimmed) title matches an
///   existing open Global record — or an earlier candidate in the same batch —
///   is skipped (dedup guard);
/// - records are created `Open`, tagged `suggested`, attributed to
///   `RecordActor::Extractor` with the user's verbatim excerpt; NO schedules or
///   reminders are created for suggested records (no schedule-bridge
///   involvement) — the agenda renders them for confirmation.
async fn persist_ledger_candidates(
    ledger: &LedgerStore,
    candidates: Vec<LedgerExtractionCandidate>,
) -> Result<usize, String> {
    if candidates.is_empty() {
        return Ok(0);
    }

    let existing = ledger
        .list_records(LedgerScope::Global, None, &RecordFilter::default())
        .await
        .map_err(|error| format!("failed to list ledger records for dedup: {error}"))?;
    let mut seen_titles: HashSet<String> = existing
        .iter()
        .map(|doc| normalized_ledger_title(&doc.record.title))
        .collect();

    let mut writes = 0usize;
    for candidate in candidates {
        let confidence = candidate
            .confidence
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .unwrap_or_default();
        if confidence != "high" && confidence != "medium" {
            continue;
        }
        let title = candidate.title.trim().to_string();
        if title.is_empty() || title.chars().count() > MAX_RECORD_TITLE_LEN {
            continue;
        }
        if !seen_titles.insert(normalized_ledger_title(&title)) {
            continue;
        }

        let kind = RecordKind::parse(&candidate.kind).unwrap_or_default();
        let mut record = LedgerRecord::new(new_record_id(), kind, title);
        record.scope = LedgerScope::Global;
        record.source.session_id = candidate
            .session_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        record.source.created_by = RecordActor::Extractor;
        record.source.excerpt = candidate
            .excerpt
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        record.tags = vec!["suggested".to_string()];
        record.time.due_at = parse_candidate_timestamp(candidate.due_at.as_deref());
        record.time.starts_at = parse_candidate_timestamp(candidate.starts_at.as_deref());

        let title_for_error = record.title.clone();
        ledger.write_record(record, None).await.map_err(|error| {
            format!("failed to persist ledger candidate '{title_for_error}': {error}")
        })?;
        writes += 1;
    }

    Ok(writes)
}

async fn collect_stream_text(
    provider: Arc<dyn LLMProvider>,
    model: &str,
    prompt: String,
) -> Result<String, String> {
    let content = collect_complete_stream_text(provider, model, prompt).await?;
    Ok(truncate_chars(&content, DREAM_MAX_SUMMARY_CHARS))
}

async fn collect_complete_stream_text(
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
        required_tool: None,
        responses: None,
        request_purpose: Some("auto_dream".to_string()),
        cache: None,
    };

    let mut stream = provider
        .chat_stream_with_options(&messages, &[], Some(8192), model, Some(&options))
        .await
        .map_err(|error| format!("auto-dream provider call failed: {error}"))?;

    let mut content = String::new();
    let mut completed = false;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(LLMChunk::Token(text)) => content.push_str(&text),
            Ok(LLMChunk::Done) => {
                completed = true;
                break;
            }
            Ok(_) => {}
            // A partial stream is not a complete response and must never be
            // treated as one: truncation at an error boundary could hide the
            // remainder of a credential from the privacy check below.
            Err(error) => return Err(format!("auto-dream stream failed: {error}")),
        }
    }

    if !completed {
        return Err("auto-dream stream ended before completion".to_string());
    }

    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Err("auto-dream returned empty content".to_string());
    }
    Ok(trimmed.to_string())
}

async fn read_existing_dream_for_scope(
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
) -> Result<Option<String>, String> {
    memory
        .read_dream_snapshot(scope, project_key)
        .await
        .map(|result| result.snapshot.map(|snapshot| snapshot.content))
        .map_err(|error| format!("failed to read Dream snapshot: {error}"))
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

async fn build_dream_notebook_body(
    provider: &Arc<dyn LLMProvider>,
    model: &str,
    source_window: &DreamSourceWindow,
    generation_mode: DreamGenerationMode,
) -> Result<String, String> {
    let prompt = match generation_mode {
        DreamGenerationMode::Rebuild => {
            tracing::info!(
                target: DREAM_TRACING_TARGET,
                event = "rebuild_attempt",
                model = model,
                session_count = source_window.sessions.len(),
                durable_memory_index_present = source_window.durable_memory_index.is_some(),
                "Attempting full rebuild Dream synthesis"
            );
            let sanitized_memory_index = source_window
                .durable_memory_index
                .as_deref()
                .map(sanitize_extraction_source);
            build_rebuild_consolidation_prompt(
                sanitized_memory_index.as_deref(),
                &to_consolidation_sessions(&source_window.sessions),
            )
        }
        DreamGenerationMode::Incremental => {
            build_consolidation_prompt(&to_consolidation_sessions(&source_window.sessions))
        }
    };
    let raw_body = collect_complete_stream_text(provider.clone(), model, prompt).await?;
    if !extraction_sources_are_secret_safe(&[raw_body.as_str()]) {
        return Err("auto-dream rejected secret-like notebook output".to_string());
    }
    let body = normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)?;

    if extraction_sources_are_secret_safe(&[body.as_str()]) {
        Ok(body)
    } else {
        Err("auto-dream rejected secret-like notebook output".to_string())
    }
}

/// Decide the `Last full rebuild at:` marker line for the dream notebook.
///
/// Stamps `now` on a forced periodic pass, OR to BOOTSTRAP the marker on the
/// first-ever grounded `Rebuild` when none exists yet — a fresh install never had
/// `last_full_rebuild_at`, and `should_force_full_rebuild` returns false while it's
/// `None`, so without the bootstrap the periodic wide-window sweep could never
/// fire (#261). Once seeded, ordinary (non-forced) passes PRESERVE the existing
/// marker so the 30-day timer isn't reset every tick; nothing is emitted while
/// there's no marker to preserve and no durable memory to ground a Rebuild on.
fn full_rebuild_marker_line(
    force_full_rebuild: bool,
    generation_mode: DreamGenerationMode,
    last_full_rebuild_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    if force_full_rebuild
        || (matches!(generation_mode, DreamGenerationMode::Rebuild)
            && last_full_rebuild_at.is_none())
    {
        format!("Last full rebuild at: {}\n", now.to_rfc3339())
    } else if let Some(existing_rebuild_at) = last_full_rebuild_at {
        format!(
            "Last full rebuild at: {}\n",
            existing_rebuild_at.to_rfc3339()
        )
    } else {
        String::new()
    }
}

async fn run_auto_dream_once_for_scope(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
    require_auto_dream_enabled: bool,
    project_resolver: Option<&ProjectContextResolver>,
) -> Result<Option<AutoDreamRunResult>, String> {
    let scope_label = match scope {
        MemoryScope::Global => "global",
        MemoryScope::Project => "project",
        MemoryScope::Session => "session",
    };

    let config_snapshot = ctx.config.read().await.clone();
    let memory_cfg = config_snapshot.memory().clone().unwrap_or_default();
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

    // NOTE: the background model is resolved AFTER the candidate-session check
    // below, so an idle default-on instance with no model configured returns
    // quietly (no candidate sessions) instead of warning every tick. Mirrors the
    // gardener, which checks its worklist before resolving a model.
    let now = Utc::now();
    let existing = read_existing_dream_for_scope(memory, scope, project_key).await?;
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
            collect_candidate_sessions_for_project(ctx, project_key, since).await
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
            existing_dream_present = existing.is_some(),
            "Skipping Dream generation because there are no candidate sessions"
        );
        return Ok(None);
    }

    // There IS work — now resolve the background model (and provider when using
    // ProviderModelRef). Doing this after the session check keeps an idle default-on
    // instance without a model quiet; a "no model" warn here means real work exists
    // that we can't do.
    let resolved_background =
        if config_snapshot.features.provider_model_ref && config_snapshot.defaults.is_some() {
            crate::model_config_helper::resolve_background_model(
                &config_snapshot,
                config_snapshot.effective_default_provider(),
                &ctx.provider_registry,
            )
            .map(|resolved| (resolved.provider, resolved.model_name))
        } else {
            config_snapshot
                .get_memory_background_model()
                .map(|model| (ctx.provider.clone(), model))
        };
    let Some((bg_provider, model)) = resolved_background else {
        tracing::warn!(
            target: DREAM_TRACING_TARGET,
            event = "run_skip",
            reason = "no_background_model",
            scope = scope_label,
            project_key = project_key.unwrap_or(""),
            "[auto_dream] skipped: no background/fast/chat model configured"
        );
        return Ok(None);
    };

    tracing::info!(
        target: DREAM_TRACING_TARGET,
        event = "run_start",
        scope = scope_label,
        project_key = project_key.unwrap_or(""),
        model = model.as_str(),
        session_count = sessions.len(),
        existing_dream_present = existing.is_some(),
        force_full_rebuild = force_full_rebuild,
        require_auto_dream_enabled = require_auto_dream_enabled,
        "Starting Dream generation run"
    );

    // One extraction model call drives both durable outputs. Jiandu facts are
    // written first, the Bamboo-owned Ledger second, and only then are source
    // sessions marked extracted. Dream synthesis must observe this completed
    // canonical state, never the pre-extraction MEMORY view.
    let extraction_sessions =
        collect_candidate_session_contexts_from_sessions(ctx, memory, sessions.clone()).await;
    let ledger = ledger_store_for_context(ctx);
    let extraction_writes = extract_and_persist_durable_candidates_with_project_resolver(
        ctx,
        &bg_provider,
        memory,
        &ledger,
        &model,
        &extraction_sessions,
        project_resolver,
        scope == MemoryScope::Project,
    )
    .await?;

    // Dream is a derived Jiandu snapshot. Capture the source generation after
    // all extraction writes, then read canonical MEMORY for the single synthesis
    // attempt. Any canonical write from this point onward changes the generation,
    // so publication fails CAS instead of marking older input as fresh.
    let source_generation = memory
        .current_scope_generation(scope, project_key)
        .await
        .map_err(|error| format!("failed to capture Dream source generation: {error}"))?;
    let durable_memory_index =
        read_durable_memory_index_for_scope(memory, scope, project_key).await?;

    // The notebook is a VIEW of durable memory (L3): rebuild it from the canonical
    // durable memory index whenever any durable memory exists — grounded in the
    // source of truth — and only bootstrap from recent sessions when there is no
    // durable memory to ground on yet. `force_full_rebuild` additionally widens the
    // session window (see `since`) on the periodic pass. The retired `Refine` mode
    // rewrote the notebook from its own prior prose, drifting from durable truth.
    let generation_mode = if force_full_rebuild || durable_memory_index.is_some() {
        DreamGenerationMode::Rebuild
    } else {
        DreamGenerationMode::Incremental
    };
    let source_window = DreamSourceWindow {
        existing_dream: existing,
        durable_memory_index,
        sessions,
    };
    let notebook_body =
        build_dream_notebook_body(&bg_provider, &model, &source_window, generation_mode).await?;
    let last_full_rebuild_line = full_rebuild_marker_line(
        force_full_rebuild,
        generation_mode,
        last_full_rebuild_at,
        now,
    );
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

    let snapshot = memory
        .publish_dream_snapshot(scope, project_key, &source_generation, &final_note)
        .await
        .map_err(|error| format!("failed to publish Dream snapshot: {error}"))?;
    let notebook_chars = final_note.chars().count();

    tracing::info!(
        target: DREAM_TRACING_TARGET,
        event = "run_complete",
        scope = scope_label,
        project_key = project_key.unwrap_or(""),
        model = model.as_str(),
        session_count = source_window.sessions.len(),
        existing_dream_present = source_window.existing_dream.is_some(),
        durable_memory_index_present = source_window.durable_memory_index.is_some(),
        generation_mode = match generation_mode {
            DreamGenerationMode::Incremental => "incremental",
            DreamGenerationMode::Rebuild => "rebuild",
        },
        notebook_chars = notebook_chars,
        durable_candidates_persisted = extraction_writes.memory,
        ledger_candidates_persisted = extraction_writes.ledger,
        generated_at = snapshot.generated_at.as_str(),
        source_generation = snapshot.source_generation.as_str(),
        "Dream generation run completed"
    );

    Ok(Some(AutoDreamRunResult {
        used_model: model,
        session_count: source_window.sessions.len(),
        generated_at: snapshot.generated_at,
        source_generation: snapshot.source_generation,
        notebook_chars,
    }))
}

async fn run_auto_dream_once_with_store(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
) -> Result<Option<AutoDreamRunResult>, String> {
    run_auto_dream_once_for_scope(ctx, memory, MemoryScope::Global, None, true, None).await
}

pub async fn run_auto_dream_once(
    ctx: &AutoDreamContext,
) -> Result<Option<AutoDreamRunResult>, String> {
    let memory = memory_store_for_context(ctx);
    run_auto_dream_once_with_store(ctx, &memory).await
}

pub async fn run_auto_dream_once_with_project_resolver(
    ctx: &AutoDreamContext,
    project_resolver: &ProjectContextResolver,
) -> Result<Option<AutoDreamRunResult>, String> {
    let memory = memory_store_for_context(ctx);
    run_auto_dream_once_for_scope(
        ctx,
        &memory,
        MemoryScope::Global,
        None,
        true,
        Some(project_resolver),
    )
    .await
}

/// Run Project Dream against the first-class Project-home memory layout.
pub async fn run_project_auto_dream_once_for_project(
    ctx: &AutoDreamContext,
    project_id: &bamboo_domain::ProjectId,
) -> Result<Option<AutoDreamRunResult>, String> {
    let memory = memory_store_for_context(ctx).for_project(project_id);
    run_project_auto_dream_once_with_store(ctx, &memory, project_id.as_str()).await
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
    run_auto_dream_once_for_scope(
        ctx,
        memory,
        MemoryScope::Project,
        Some(project_key),
        false,
        None,
    )
    .await
}

pub fn spawn_auto_dream_task(ctx: AutoDreamContext) {
    spawn_auto_dream_task_inner(ctx, None);
}

pub fn spawn_auto_dream_task_with_project_resolver(
    ctx: AutoDreamContext,
    project_resolver: ProjectContextResolver,
) {
    spawn_auto_dream_task_inner(ctx, Some(project_resolver));
}

fn spawn_auto_dream_task_inner(
    ctx: AutoDreamContext,
    project_resolver: Option<ProjectContextResolver>,
) {
    tokio::spawn(async move {
        let interval_secs = ctx
            .config
            .read()
            .await
            .memory()
            .as_ref()
            .map(|memory| memory.auto_dream_interval_secs)
            .filter(|secs| *secs > 0)
            // Fall back to the config default (single source of truth for the
            // 30-minute cadence) when memory config is absent or set to 0.
            .unwrap_or_else(|| bamboo_config::MemoryConfig::default().auto_dream_interval_secs);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            let result = match project_resolver.as_ref() {
                Some(resolver) => run_auto_dream_once_with_project_resolver(&ctx, resolver).await,
                None => run_auto_dream_once(&ctx).await,
            };
            if let Err(error) = result {
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream;

    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::{
        CompressionEvent, CompressionTriggerType, FunctionCall, ImageUrlRef, MessagePart, ToolCall,
    };
    use bamboo_domain::{ProjectId, ProjectResourceSummary, WorkspaceBinding};
    use bamboo_llm::{LLMError, LLMStream};

    struct StaticProjectSource(crate::project_context::ProjectDescriptor);

    #[async_trait]
    impl crate::project_context::ProjectContextSource for StaticProjectSource {
        async fn find_project(
            &self,
            project_id: &ProjectId,
        ) -> Result<
            Option<crate::project_context::ProjectDescriptor>,
            crate::project_context::ProjectContextError,
        > {
            Ok((&self.0.id == project_id).then(|| self.0.clone()))
        }
    }

    fn config_with_memory(memory: bamboo_config::MemoryConfig) -> Config {
        let mut config = Config::default();
        *config.memory_mut() = Some(memory);
        config
    }

    async fn publish_test_dream(
        memory: &MemoryStore,
        scope: MemoryScope,
        project_key: Option<&str>,
        content: &str,
    ) {
        let source_generation = memory
            .current_scope_generation(scope, project_key)
            .await
            .expect("read test source generation");
        memory
            .publish_dream_snapshot(scope, project_key, &source_generation, content)
            .await
            .expect("publish test Dream snapshot");
    }

    async fn read_test_dream(
        memory: &MemoryStore,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> Option<String> {
        memory
            .read_dream_snapshot(scope, project_key)
            .await
            .expect("read test Dream snapshot")
            .snapshot
            .map(|snapshot| snapshot.content)
    }

    #[test]
    fn full_rebuild_marker_bootstraps_on_first_grounded_rebuild() {
        let now = "2026-07-08T12:00:00Z".parse::<DateTime<Utc>>().unwrap();

        // #261: a fresh install (no prior marker) doing its first grounded Rebuild
        // must SEED the marker with `now`, so the 30-day periodic cadence has a
        // start point instead of never firing.
        let line = full_rebuild_marker_line(false, DreamGenerationMode::Rebuild, None, now);
        assert_eq!(
            line,
            format!("Last full rebuild at: {}\n", now.to_rfc3339())
        );
    }

    #[test]
    fn full_rebuild_marker_preserves_existing_on_non_forced_pass() {
        let now = "2026-07-08T12:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let existing = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        // Once seeded, an ordinary (non-forced) pass must PRESERVE the marker, not
        // reset it to `now` — otherwise the timer would restart every tick and the
        // periodic sweep would never come due.
        let line =
            full_rebuild_marker_line(false, DreamGenerationMode::Rebuild, Some(existing), now);
        assert_eq!(
            line,
            format!("Last full rebuild at: {}\n", existing.to_rfc3339())
        );
    }

    #[test]
    fn full_rebuild_marker_stamps_now_on_forced_pass() {
        let now = "2026-07-08T12:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let existing = "2026-06-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        // The periodic forced pass re-stamps `now`, advancing the cadence.
        let line =
            full_rebuild_marker_line(true, DreamGenerationMode::Rebuild, Some(existing), now);
        assert_eq!(
            line,
            format!("Last full rebuild at: {}\n", now.to_rfc3339())
        );
    }

    #[test]
    fn full_rebuild_marker_absent_when_incremental_and_no_prior_marker() {
        let now = "2026-07-08T12:00:00Z".parse::<DateTime<Utc>>().unwrap();

        // No durable memory yet (Incremental bootstrap) and no prior marker: emit
        // nothing — there's no grounded rebuild to anchor the cadence to.
        let line = full_rebuild_marker_line(false, DreamGenerationMode::Incremental, None, now);
        assert_eq!(line, String::new());
    }

    fn test_registry() -> Arc<ProviderRegistry> {
        Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()))
    }

    fn test_time(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("valid test timestamp")
    }

    fn retrieval_event(id: &str, created_at: DateTime<Utc>) -> CompressionEvent {
        let mut event = CompressionEvent::new(
            0,
            0,
            0.0,
            0.0,
            0,
            CompressionTriggerType::Auto,
            1.0,
            None,
            0,
        );
        event.id = id.to_string();
        event.created_at = created_at;
        event.kind = CompressionEventKind::RetrievalWindow;
        event
    }

    fn message_at(mut message: Message, id: &str, created_at: DateTime<Utc>) -> Message {
        message.id = id.to_string();
        message.created_at = created_at;
        message
    }

    #[derive(Clone)]
    struct SequenceProvider {
        responses: Arc<Mutex<Vec<String>>>,
        prompts: Arc<Mutex<Vec<String>>>,
    }

    impl SequenceProvider {
        fn new(responses: Vec<String>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
                prompts: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn recorded_prompts(&self) -> Vec<String> {
            self.prompts.lock().expect("lock poisoned").clone()
        }
    }

    #[derive(Clone)]
    struct ScriptedProvider {
        steps: Arc<Mutex<Vec<Result<String, String>>>>,
        prompts: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedProvider {
        fn new(steps: Vec<Result<String, String>>) -> Self {
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
    impl LLMProvider for ScriptedProvider {
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
            match self.steps.lock().expect("lock poisoned").remove(0) {
                Ok(text) => Ok(Box::pin(stream::iter(vec![
                    Ok(LLMChunk::Token(text)),
                    Ok(LLMChunk::Done),
                ]))),
                Err(message) => Err(LLMError::Api(message)),
            }
        }
    }

    fn extraction_response(title: &str, content: &str) -> String {
        serde_json::json!({
            "candidates": [{
                "title": title,
                "type": "reference",
                "scope": "global",
                "content": content,
                "tags": ["auto-dream-test"],
                "session_id": "source-session-0001",
                "confidence": "high"
            }],
            "ledger_candidates": [],
            "source_exhausted": true
        })
        .to_string()
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
            let text = self.responses.lock().expect("lock poisoned").remove(0);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(text)),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[derive(Clone)]
    struct IncompleteProvider;

    #[async_trait]
    impl LLMProvider for IncompleteProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Token(
                "partial response without a terminal chunk".to_string(),
            ))])))
        }
    }

    #[derive(Clone)]
    struct CasMutatingProvider {
        responses: Arc<Mutex<Vec<String>>>,
        calls: Arc<AtomicUsize>,
        memory: MemoryStore,
    }

    #[async_trait]
    impl LLMProvider for CasMutatingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let text = self.responses.lock().expect("lock poisoned").remove(0);
            if call == 1 {
                self.memory
                    .write_memory(
                        MemoryScope::Global,
                        None,
                        bamboo_memory::memory_store::DurableMemoryType::Feedback,
                        "Concurrent canonical update",
                        "This durable fact lands after Dream captured its source generation.",
                        &["concurrency".to_string()],
                        Some("session-cas-dream"),
                        "test",
                        false,
                        None,
                    )
                    .await
                    .expect("write concurrent canonical memory");
            }
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(text)),
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
    async fn complete_stream_text_rejects_clean_eof_without_done() {
        let provider: Arc<dyn LLMProvider> = Arc::new(IncompleteProvider);
        let error = collect_complete_stream_text(provider, "test-model", "prompt".to_string())
            .await
            .expect_err("clean EOF without Done must remain retryable");
        assert_eq!(error, "auto-dream stream ended before completion");
    }

    #[test]
    fn outline_sanitizes_full_message_before_truncation() {
        const SECRET_PREFIX: &str = "mF9/Bx7Qa2cD8";
        let secret = "mF9/Bx7Qa2cD8Zp4Ln6Rt3Vy5Kw1Hs0Je";
        let mut session = bamboo_agent_core::Session::new("session-boundary", "model");
        session.add_message(Message::user(format!(
            "{}{}",
            "ordinary ".repeat(32),
            secret
        )));

        let outline = derive_sanitized_session_outline(&session).expect("sanitized outline");
        assert!(outline.contains(crate::auto_dream_privacy::REDACTED_EXTRACTION_SOURCE));
        assert!(
            !outline.contains(SECRET_PREFIX),
            "a credential prefix crossed the outline truncation boundary"
        );
    }

    #[test]
    fn outline_rejects_credentials_split_across_recent_messages() {
        let mut session = bamboo_agent_core::Session::new("session-message-split", "model");
        session.add_message(Message::user("Password"));
        session.add_message(Message::assistant("hunter2", None));

        assert!(
            derive_sanitized_session_outline(&session).as_deref()
                == Some(REDACTED_EXTRACTION_SOURCE),
            "a credential split across recent messages was not redacted"
        );
    }

    #[test]
    fn outline_sanitizes_task_fields_before_prompt_truncation() {
        const SECRET_PREFIX: &str = "mF9/Bx7Qa2cD8";
        let secret = "mF9/Bx7Qa2cD8Zp4Ln6Rt3Vy5Kw1Hs0Je";
        let mut session = bamboo_agent_core::Session::new("session-task-boundary", "model");
        let mut item = bamboo_domain::TaskItem {
            id: "task-1".to_string(),
            description: "Safe task description".to_string(),
            ..bamboo_domain::TaskItem::default()
        };
        item.notes = format!("{}{}", "ordinary ".repeat(16), secret);
        let now = Utc::now();
        session.task_list = Some(bamboo_domain::TaskList {
            session_id: session.id.clone(),
            title: "Safe task list".to_string(),
            items: vec![item],
            created_at: now,
            updated_at: now,
        });

        let outline = derive_sanitized_session_outline(&session).expect("sanitized outline");
        assert!(
            outline == REDACTED_EXTRACTION_SOURCE,
            "task-list source was not redacted before truncation"
        );
        assert!(
            !outline.contains(SECRET_PREFIX),
            "a credential prefix crossed the task-list truncation boundary"
        );
    }

    #[test]
    fn outline_rejects_credentials_split_across_task_fields() {
        let mut session = bamboo_agent_core::Session::new("session-task-split", "model");
        let item = bamboo_domain::TaskItem {
            id: "API".to_string(),
            description: "key".to_string(),
            notes: "hunter2".to_string(),
            ..bamboo_domain::TaskItem::default()
        };
        let now = Utc::now();
        session.task_list = Some(bamboo_domain::TaskList {
            session_id: session.id.clone(),
            title: "Safe task list".to_string(),
            items: vec![item],
            created_at: now,
            updated_at: now,
        });

        assert!(
            derive_sanitized_session_outline(&session).as_deref()
                == Some(REDACTED_EXTRACTION_SOURCE),
            "a credential split across task fields was not redacted"
        );
    }

    #[test]
    fn outline_keeps_ordinary_multi_item_task_list() {
        let mut session =
            bamboo_agent_core::Session::new("0123456789abcdef0123456789abcdef", "model");
        let items = (1..=3)
            .map(|index| bamboo_domain::TaskItem {
                id: format!("task-{index}"),
                description: format!("Complete ordinary work item {index}"),
                notes: format!("Verified ordinary progress {index}"),
                ..bamboo_domain::TaskItem::default()
            })
            .collect();
        let now = Utc::now();
        session.task_list = Some(bamboo_domain::TaskList {
            session_id: session.id.clone(),
            title: "Safe task list".to_string(),
            items,
            created_at: now,
            updated_at: now,
        });

        let outline = derive_sanitized_session_outline(&session).expect("safe task outline");
        assert_ne!(outline, REDACTED_EXTRACTION_SOURCE);
        assert!(outline.contains("Complete ordinary work item"));
    }

    #[test]
    fn retrieval_delta_keeps_archived_content_beyond_the_recent_outline() {
        let mut session = Session::new("retrieval-delta-old", "model");
        session.messages.push(message_at(
            Message::system("SYSTEM_POLICY_MUST_NOT_APPEAR"),
            "system",
            test_time(1),
        ));
        session
            .compression_events
            .push(retrieval_event("event-old", test_time(20)));
        let mut archived = message_at(
            Message::user("ARCHIVED_IDENTIFIER_ALPHA_947"),
            "archived-old",
            test_time(2),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event-old".to_string());
        session.messages.push(archived);
        for index in 0..8 {
            session.messages.push(message_at(
                Message::assistant(format!("recent message {index}"), None),
                &format!("recent-{index}"),
                test_time(30 + index),
            ));
        }

        let recent_outline = derive_session_outline(&session).expect("recent outline");
        assert!(!recent_outline.contains("ARCHIVED_IDENTIFIER_ALPHA_947"));
        let sources = session_extraction_sources(&session, None, false);
        assert_eq!(sources.len(), 2);
        let delta = sources
            .iter()
            .filter_map(Option::as_deref)
            .collect::<String>();
        assert!(delta.contains("ARCHIVED_IDENTIFIER_ALPHA_947"));
        assert!(!delta.contains("SYSTEM_POLICY_MUST_NOT_APPEAR"));
    }

    #[test]
    fn retrieval_delta_applies_strict_message_watermarks_after_migration() {
        let mut session = Session::new("retrieval-delta-watermark", "model");
        session
            .compression_events
            .push(retrieval_event("event-new", test_time(30)));
        session
            .compression_events
            .push(retrieval_event("event-old", test_time(10)));
        for (id, content, created_at, event_id) in [
            ("old-archived", "OLD_ARCHIVED", 2, Some("event-old")),
            (
                "old-message-new-event",
                "ARCHIVED_BY_NEW_EVENT",
                3,
                Some("event-new"),
            ),
            ("old-active", "OLD_ACTIVE", 19, None),
            ("equal-active", "EQUAL_ACTIVE", 20, None),
            ("new-archived", "NEW_ARCHIVED", 21, Some("event-new")),
            ("new-active", "NEW_ACTIVE", 22, None),
        ] {
            let mut message = message_at(Message::user(content), id, test_time(created_at));
            if let Some(event_id) = event_id {
                message.compressed = true;
                message.compressed_by_event_id = Some(event_id.to_string());
            }
            session.messages.push(message);
        }

        let batches =
            build_retrieval_window_extraction_batches(&session, Some(test_time(20)), true);
        assert_eq!(batches.len(), 1);
        let delta = &batches[0];
        assert!(delta.contains("ARCHIVED_BY_NEW_EVENT"));
        assert!(delta.contains("NEW_ARCHIVED"));
        assert!(delta.contains("NEW_ACTIVE"));
        assert!(!delta.contains("OLD_ARCHIVED"));
        assert!(!delta.contains("OLD_ACTIVE"));
        assert!(!delta.contains("EQUAL_ACTIVE"));
        assert!(delta.contains("eligible_retrieval_events: 1"));
        assert!(
            delta.find("NEW_ARCHIVED").expect("archived position")
                < delta.find("NEW_ACTIVE").expect("active position")
        );
    }

    #[test]
    fn first_retrieval_transition_includes_old_retained_messages() {
        let mut session = Session::new("retrieval-first-transition", "model");
        session
            .compression_events
            .push(retrieval_event("event-first", test_time(30)));
        let mut archived = message_at(
            Message::user("OLD_NEWLY_ARCHIVED"),
            "old-archived",
            test_time(2),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event-first".to_string());
        session.messages.push(archived);
        session.messages.push(message_at(
            Message::assistant("OLD_RETAINED_ACTIVE", None),
            "old-retained",
            test_time(3),
        ));
        session.messages.push(message_at(
            Message::user("NEW_ACTIVE"),
            "new-active",
            test_time(21),
        ));

        let batches =
            build_retrieval_window_extraction_batches(&session, Some(test_time(20)), false);
        assert_eq!(batches.len(), 1);
        assert!(batches[0].contains("OLD_NEWLY_ARCHIVED"));
        assert!(batches[0].contains("OLD_RETAINED_ACTIVE"));
        assert!(batches[0].contains("NEW_ACTIVE"));
    }

    #[test]
    fn retrieval_delta_excludes_private_and_self_history_payloads() {
        let mut session = Session::new("retrieval-delta-private-fields", "model");
        session
            .compression_events
            .push(retrieval_event("event", test_time(10)));
        session.messages.push(message_at(
            Message::system("SYSTEM_SECRET"),
            "system",
            test_time(11),
        ));

        let mut ordinary = message_at(
            Message::assistant("SAFE_VISIBLE_CONTENT", None),
            "ordinary",
            test_time(12),
        );
        ordinary.reasoning = Some("REASONING_SECRET".to_string());
        ordinary.reasoning_signature = Some("SIGNATURE_SECRET".to_string());
        ordinary.content_parts = Some(vec![MessagePart::ImageUrl {
            image_url: ImageUrlRef {
                url: "data:image/png;base64,IMAGE_BYTES_SECRET".to_string(),
                detail: None,
            },
        }]);
        session.messages.push(ordinary);
        session.messages.push(message_at(
            Message::user("OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz"),
            "credential-user",
            test_time(13),
        ));

        let history_call_id = "history-call";
        session.messages.push(message_at(
            Message::assistant(
                "SEARCH_QUERY_SECRET",
                Some(vec![ToolCall {
                    id: history_call_id.to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "session_history_current".to_string(),
                        arguments: serde_json::json!({
                            "action": "search_current",
                            "query": "SEARCH_QUERY_ARGUMENT_SECRET"
                        })
                        .to_string(),
                    },
                }]),
            ),
            "history-call-message",
            test_time(14),
        ));
        session.messages.push(message_at(
            Message::tool_result(history_call_id, "SEARCH_RESULT_SECRET"),
            "history-result-message",
            test_time(15),
        ));

        let note_call_id = "session-note-call";
        session.messages.push(message_at(
            Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: note_call_id.to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "session_note".to_string(),
                        arguments: serde_json::json!({"action": "read"}).to_string(),
                    },
                }]),
            ),
            "session-note-call-message",
            test_time(16),
        ));
        session.messages.push(message_at(
            Message::tool_result(
                note_call_id,
                serde_json::json!({
                    "action": "read",
                    "exists": true,
                    "content": "SESSION_NOTE_CREDENTIAL_SECRET",
                    "path": "/sensitive/session/note/path",
                    "length_chars": 30,
                    "body_truncated": false,
                    "max_chars": 12000
                })
                .to_string(),
            ),
            "session-note-result",
            test_time(17),
        ));
        session.messages.push(message_at(
            Message::tool_result("untrusted-tool-call", "TOOL_CREDENTIAL_SECRET"),
            "untrusted-tool-result",
            test_time(18),
        ));

        let batches = build_retrieval_window_extraction_batches(&session, None, false);
        assert_eq!(batches.len(), 1);
        let delta = &batches[0];
        assert!(delta.contains("SAFE_VISIBLE_CONTENT"));
        assert!(delta.contains("session_note"));
        assert!(delta.contains("length_chars"));
        assert!(delta.contains(REDACTED_EXTRACTION_SOURCE));
        for excluded in [
            "SYSTEM_SECRET",
            "REASONING_SECRET",
            "SIGNATURE_SECRET",
            "IMAGE_BYTES_SECRET",
            "SEARCH_QUERY_SECRET",
            "SEARCH_QUERY_ARGUMENT_SECRET",
            "SEARCH_RESULT_SECRET",
            "SESSION_NOTE_CREDENTIAL_SECRET",
            "/sensitive/session/note/path",
            "TOOL_CREDENTIAL_SECRET",
            "sk-proj-abcdefghijklmnopqrstuvwxyz",
        ] {
            assert!(
                !delta.contains(excluded),
                "unexpected private field: {excluded}"
            );
        }
    }

    #[test]
    fn retrieval_delta_ordering_caps_and_restart_are_deterministic() {
        let mut session = Session::new("retrieval-delta-caps", "model");
        session
            .compression_events
            .push(retrieval_event("event", test_time(1)));
        for index in 0..66 {
            session.messages.push(message_at(
                Message::user(format!("ORDER_{index:03}")),
                &format!("message-{index:03}"),
                test_time(10 + index),
            ));
        }

        let batches = build_retrieval_window_extraction_batches(&session, None, false);
        assert_eq!(batches.len(), 9);
        assert!(batches[0].contains("batch: 1/9"));
        assert!(batches[0].contains("source_items_in_batch: 8"));
        assert!(batches[1].contains("continuation_overlap_content_suffix: \"ORDER_007\""));
        assert!(batches[8].contains("batch: 9/9"));
        assert!(batches[8].contains("source_items_in_batch: 2"));
        assert!(batches.iter().all(|batch| {
            batch.contains("eligible_messages: 66")
                && batch.contains("truncated: false")
                && batch.chars().count() <= RETRIEVAL_EXTRACTION_MAX_CHARS
        }));
        let all_batches = batches.concat();
        for index in 0..66 {
            assert!(all_batches.contains(&format!("ORDER_{index:03}")));
        }

        let restored: Session = serde_json::from_slice(
            &serde_json::to_vec(&session).expect("serialize retrieval Session"),
        )
        .expect("restore retrieval Session");
        assert_eq!(
            build_retrieval_window_extraction_batches(&restored, None, false),
            build_retrieval_window_extraction_batches(&session, None, false)
        );
        assert!(
            build_retrieval_window_extraction_batches(&restored, Some(test_time(100)), true)
                .is_empty()
        );
    }

    #[test]
    fn extraction_source_preserves_summary_first_and_outline_fallback() {
        let mut summary_session = Session::new("summary-first", "model");
        summary_session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "EXACT_SUMMARY_SOURCE",
            1,
            10,
        ));
        summary_session
            .compression_events
            .push(retrieval_event("event", test_time(10)));
        summary_session
            .messages
            .push(Message::user("retrieval content must not override summary"));
        assert_eq!(
            session_extraction_sources(&summary_session, None, false),
            vec![Some("EXACT_SUMMARY_SOURCE".to_string())]
        );

        let mut ordinary = Session::new("ordinary-outline", "model");
        ordinary
            .messages
            .push(Message::user("ORDINARY_RECENT_OUTLINE"));
        let sources = session_extraction_sources(&ordinary, None, false);
        assert_eq!(sources.len(), 1);
        let source = sources[0].as_deref().expect("outline");
        assert!(source.contains("ORDINARY_RECENT_OUTLINE"));
        assert!(!source.contains("Retrieval-window extraction delta"));
    }

    #[tokio::test]
    async fn candidate_collection_keeps_retrieval_delta_and_topics_separate() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let now = Utc::now();
        let watermark = now - chrono::Duration::seconds(20);
        let mut session = Session::new("retrieval-collection", "model");
        session.title = "Retrieval collection".to_string();
        session.compression_events.push(retrieval_event(
            "event-new",
            now - chrono::Duration::seconds(10),
        ));
        let mut archived = message_at(
            Message::user("ARCHIVED_AFTER_WATERMARK_EVENT"),
            "archived",
            now - chrono::Duration::hours(1),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event-new".to_string());
        session.messages.push(archived);
        session.messages.push(message_at(
            Message::assistant("ACTIVE_BEFORE_WATERMARK", None),
            "old-active",
            watermark - chrono::Duration::seconds(1),
        ));
        session.updated_at = now;
        storage
            .save_session(&session)
            .await
            .expect("save retrieval Session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .mark_session_extracted("retrieval-collection", &watermark.to_rfc3339())
            .await
            .expect("write extraction watermark");
        memory
            .write_session_topic(
                "retrieval-collection",
                "continuity",
                "SESSION_TOPIC_REMAINS_SEPARATE",
            )
            .await
            .expect("write Session topic");
        memory
            .write_session_topic(
                "retrieval-collection",
                "provider-auth",
                "OPENAI_API_KEY=sk-proj-topicsecretabcdefghijkl",
            )
            .await
            .expect("write sensitive Session topic");
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: Arc::new(SequenceProvider::new(Vec::<String>::new())),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };

        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            now - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 2);
        let source = contexts[0].summary.as_deref().expect("retrieval source");
        assert!(source.contains("ARCHIVED_AFTER_WATERMARK_EVENT"));
        assert!(source.contains("ACTIVE_BEFORE_WATERMARK"));
        assert!(contexts[0].topics.is_empty());
        assert!(contexts[1].summary.is_none());
        assert_eq!(
            contexts[1].topics,
            vec![
                (
                    "continuity".to_string(),
                    "SESSION_TOPIC_REMAINS_SEPARATE".to_string()
                ),
                (
                    REDACTED_EXTRACTION_SOURCE.to_string(),
                    REDACTED_EXTRACTION_SOURCE.to_string()
                )
            ]
        );
        assert!(!extraction_prompt(&contexts[0]).contains("SESSION_TOPIC_REMAINS_SEPARATE"));
        assert!(extraction_prompt(&contexts[1]).contains("SESSION_TOPIC_REMAINS_SEPARATE"));
        assert!(!extraction_prompt(&contexts[1]).contains("sk-proj-topicsecretabcdefghijkl"));
    }

    #[tokio::test]
    async fn legacy_retrieval_watermark_requires_one_explicit_source_migration() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let now = Utc::now();
        let mut session = Session::new("retrieval-marker-migration", "model");
        session.title = "Retrieval marker migration".to_string();
        session.compression_events.push(retrieval_event(
            "legacy-event",
            now - chrono::Duration::minutes(5),
        ));
        let mut archived = message_at(
            Message::user("LEGACY_ARCHIVED_SOURCE"),
            "legacy-archived",
            now - chrono::Duration::hours(2),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("legacy-event".to_string());
        session.messages.push(archived);
        session.messages.push(message_at(
            Message::assistant("LEGACY_RETAINED_SOURCE", None),
            "legacy-retained",
            now - chrono::Duration::hours(1),
        ));
        session.updated_at = now;
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .mark_session_extracted(&session.id, &now.to_rfc3339())
            .await
            .expect("seed legacy outline watermark");
        let sequence_provider = Arc::new(SequenceProvider::new(vec![serde_json::json!({
            "candidates": [],
            "ledger_candidates": [],
            "source_exhausted": true
        })
        .to_string()]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let since = now - chrono::Duration::hours(24);

        let contexts = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(contexts.len(), 1, "missing marker must force migration");
        let source = contexts[0].summary.as_deref().expect("migration source");
        assert!(source.contains("LEGACY_ARCHIVED_SOURCE"));
        assert!(source.contains("LEGACY_RETAINED_SOURCE"));
        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &LedgerStore::new(temp_dir.path()),
            "fast-model",
            &contexts,
        )
        .await
        .expect("migration extraction");
        assert_eq!(sequence_provider.recorded_prompts().len(), 1);
        assert!(
            retrieval_source_is_acknowledged(&context, &session, Some(now))
                .await
                .expect("read retrieval source marker")
        );
        assert!(
            collect_candidate_session_contexts(&context, &memory, since)
                .await
                .is_empty(),
            "the explicit marker must make migration one-shot"
        );
    }

    #[tokio::test]
    async fn dream_synthesis_sanitizes_index_and_rejects_secret_like_output() {
        const SECRET: &str = "API key: hunter2";
        const SAFE_BODY: &str = "## Current durable context\n- Safe durable context\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None";
        let safe_provider = Arc::new(SequenceProvider::new(vec![SAFE_BODY.to_string()]));
        let safe_provider_handle: Arc<dyn LLMProvider> = safe_provider.clone();
        let source_window = DreamSourceWindow {
            existing_dream: None,
            durable_memory_index: Some(SECRET.to_string()),
            sessions: Vec::new(),
        };

        build_dream_notebook_body(
            &safe_provider_handle,
            "fast-model",
            &source_window,
            DreamGenerationMode::Rebuild,
        )
        .await
        .expect("sanitized rebuild should succeed");
        let prompt = safe_provider.recorded_prompts().remove(0);
        assert!(!prompt.contains(SECRET));
        assert!(prompt.contains(REDACTED_EXTRACTION_SOURCE));

        let private_body = SAFE_BODY.replace("Safe durable context", SECRET);
        let private_provider: Arc<dyn LLMProvider> =
            Arc::new(SequenceProvider::new(vec![private_body]));
        let error = build_dream_notebook_body(
            &private_provider,
            "fast-model",
            &DreamSourceWindow {
                existing_dream: None,
                durable_memory_index: None,
                sessions: Vec::new(),
            },
            DreamGenerationMode::Incremental,
        )
        .await
        .expect_err("secret-like Dream output must not reach Jiandu");
        assert_eq!(error, "auto-dream rejected secret-like notebook output");

        const OPAQUE_SECRET: &str = "mF9/Bx7Qa2cD8/Zp4Ln6Rt3Vy5Kw1Hs0Je";
        let prelude = format!("{SAFE_BODY}\n\n");
        let secret_start = DREAM_MAX_SUMMARY_CHARS - 12;
        let filler_len = secret_start - prelude.chars().count() - 1;
        let boundary_body = format!("{prelude}{} {OPAQUE_SECRET}", "a".repeat(filler_len));
        assert_eq!(
            boundary_body.find(OPAQUE_SECRET),
            Some(secret_start),
            "fixture must place the credential across the old truncation boundary"
        );
        let boundary_provider: Arc<dyn LLMProvider> =
            Arc::new(SequenceProvider::new(vec![boundary_body]));
        let error = build_dream_notebook_body(
            &boundary_provider,
            "fast-model",
            &DreamSourceWindow {
                existing_dream: None,
                durable_memory_index: None,
                sessions: Vec::new(),
            },
            DreamGenerationMode::Incremental,
        )
        .await
        .expect_err("complete Dream output must be checked before truncation");
        assert_eq!(error, "auto-dream rejected secret-like notebook output");
    }

    #[tokio::test]
    async fn extraction_privacy_boundary_covers_prompt_and_both_sinks() {
        const TITLE_SECRET: &str = "OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz";
        const SUMMARY_SECRET: &str = "Authorization: Bearer AbCdEfGhIjKlMnOpQrStUvWxYz123456";
        const OUTLINE_SECRET: &str = "my token is abc";
        const SYSTEM_SECRET: &str = "postgres://user:password-value@example.test/database";
        const TOPIC_SECRET: &str = "hunter2";
        const SPLIT_LABEL: &str = "Password";
        const OPAQUE_SESSION_ID: &str = "0123456789abcdef0123456789abcdef";
        const TOOL_MARKER: &str = "ORDINARY_TOOL_RESULT_MUST_NOT_REACH_EXTRACTION";
        const BOUNDARY_SECRET_PREFIX: &str = "mF9/Bx7Qa2cD8";

        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();

        let mut summarized = bamboo_agent_core::Session::new("session-summary-private", "model");
        summarized.title = TITLE_SECRET.to_string();
        summarized.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            SUMMARY_SECRET,
            2,
            64,
        ));
        storage
            .save_session(&summarized)
            .await
            .expect("save summarized session");

        let mut split = bamboo_agent_core::Session::new("session-split-private", "model");
        split.title = SPLIT_LABEL.to_string();
        split.add_message(Message::user(
            "Ordinary activity for the split-field fixture.",
        ));
        storage
            .save_session(&split)
            .await
            .expect("save split-field session");

        let mut triple_split =
            bamboo_agent_core::Session::new("session-triple-split-private", "model");
        triple_split.title = "API".to_string();
        triple_split.add_message(Message::user(
            "Ordinary activity for the three-field fixture.",
        ));
        storage
            .save_session(&triple_split)
            .await
            .expect("save three-field session");

        let mut opaque_identifier = bamboo_agent_core::Session::new(OPAQUE_SESSION_ID, "model");
        opaque_identifier.title = "Opaque authority fixture".to_string();
        opaque_identifier.add_message(Message::user(
            "Ordinary activity for the opaque-authority fixture.",
        ));
        storage
            .save_session(&opaque_identifier)
            .await
            .expect("save opaque-authority session");

        let mut outlined = bamboo_agent_core::Session::new("session-outline-private", "model");
        outlined.title = "Ordinary outline title".to_string();
        outlined.add_message(Message::user("Keep the final response concise."));
        outlined.add_message(Message::assistant(OUTLINE_SECRET, None));
        outlined.add_message(Message::system(SYSTEM_SECRET));
        outlined.add_message(Message::tool_result("call-1", TOOL_MARKER));
        storage
            .save_session(&outlined)
            .await
            .expect("save outlined session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic("session-summary-private", "Password", TOPIC_SECRET)
            .await
            .expect("write private Session topic fixture");
        memory
            .write_session_topic("session-split-private", "database", TOPIC_SECRET)
            .await
            .expect("write split-field Session topic fixture");
        memory
            .write_session_topic("session-triple-split-private", "key", TOPIC_SECRET)
            .await
            .expect("write three-field Session topic fixture");
        memory
            .write_session_topic(
                "session-summary-private",
                "boundary",
                &format!(
                    "{}{}",
                    "ordinary ".repeat(165),
                    "mF9/Bx7Qa2cD8Zp4Ln6Rt3Vy5Kw1Hs0Je"
                ),
            )
            .await
            .expect("write truncation-boundary Session topic fixture");

        let response = serde_json::json!({
            "candidates": [
                {
                    "title": "Password",
                    "type": "reference",
                    "scope": "global",
                    "content": TOPIC_SECRET,
                    "tags": ["credential"],
                    "session_id": "source-session-0001",
                    "confidence": "high"
                },
                {
                    "title": "User prefers concise replies",
                    "type": "feedback",
                    "scope": "global",
                    "content": "The user prefers concise replies.",
                    "tags": ["preference"],
                    "session_id": "source-session-0001",
                    "confidence": "high"
                },
                {
                    "title": "Opaque authority remains attributable",
                    "type": "reference",
                    "scope": "global",
                    "content": "The opaque authority fixture remains eligible.",
                    "tags": ["provenance"],
                    "session_id": "source-session-0001",
                    "confidence": "high"
                }
            ],
            "ledger_candidates": [
                {
                    "title": "PIN",
                    "kind": "todo",
                    "excerpt": "1234",
                    "session_id": "source-session-0001",
                    "confidence": "high"
                },
                {
                    "title": "Renew passport",
                    "kind": "todo",
                    "excerpt": "I will renew my passport.",
                    "session_id": "source-session-0001",
                    "confidence": "medium"
                },
                {
                    "title": "Smuggled source identity",
                    "kind": "todo",
                    "excerpt": "This visible payload is otherwise ordinary.",
                    "session_id": "password=hunter2",
                    "confidence": "high"
                }
            ]
        })
        .to_string();
        let empty_response = serde_json::json!({
            "candidates": [],
            "ledger_candidates": [],
            "source_exhausted": true
        })
        .to_string();
        let sequence = Arc::new(SequenceProvider::new(vec![
            response,
            empty_response.clone(),
            empty_response.clone(),
            empty_response.clone(),
            empty_response,
        ]));
        let provider: Arc<dyn LLMProvider> = sequence.clone();
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };

        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 5);
        let split_context = contexts
            .iter()
            .find(|context| context.session_id == "session-split-private")
            .expect("split-field context");
        let sanitized_split = sanitized_extraction_candidate_info(
            split_context,
            "source-session-test-1".to_string(),
            None,
        );
        assert_eq!(sanitized_split.title, REDACTED_EXTRACTION_SOURCE);
        assert!(sanitized_split.summary.is_none());
        assert!(sanitized_split.topics.is_empty());
        let triple_split_context = contexts
            .iter()
            .find(|context| context.session_id == "session-triple-split-private")
            .expect("three-field context");
        let sanitized_triple_split = sanitized_extraction_candidate_info(
            triple_split_context,
            "source-session-test-2".to_string(),
            None,
        );
        assert_eq!(sanitized_triple_split.title, REDACTED_EXTRACTION_SOURCE);
        assert!(sanitized_triple_split.summary.is_none());
        assert!(sanitized_triple_split.topics.is_empty());
        let opaque_identifier_context = contexts
            .iter()
            .find(|context| context.session_id == OPAQUE_SESSION_ID)
            .expect("opaque-authority context");
        let sanitized_opaque_identifier = sanitized_extraction_candidate_info(
            opaque_identifier_context,
            "source-session-test-3".to_string(),
            None,
        );
        assert_eq!(
            sanitized_opaque_identifier.session_id,
            "source-session-test-3"
        );
        assert_eq!(
            sanitized_opaque_identifier.title,
            "Opaque authority fixture"
        );
        assert!(sanitized_opaque_identifier.summary.is_some());
        assert!(sanitized_opaque_identifier.topics.is_empty());
        let consolidation_sessions = to_consolidation_sessions(&[(
            opaque_identifier_context.entry.clone(),
            opaque_identifier_context.summary.clone(),
        )]);
        assert_eq!(consolidation_sessions[0].id, provider_session_alias(0));
        assert_eq!(consolidation_sessions[0].title, "Opaque authority fixture");
        assert!(consolidation_sessions[0].last_run_status.is_none());
        assert!(consolidation_sessions[0].summary.is_some());
        let consolidation_prompt = build_consolidation_prompt(&consolidation_sessions);
        assert!(!consolidation_prompt.contains(OPAQUE_SESSION_ID));
        assert!(consolidation_prompt.contains(&provider_session_alias(0)));
        assert!(consolidation_prompt.contains("Opaque authority fixture"));
        let ledger = LedgerStore::new(temp_dir.path());
        let writes = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("privacy-safe extraction should succeed");
        assert_eq!(
            writes,
            ExtractionWrites {
                memory: 2,
                ledger: 1
            }
        );

        let prompts = sequence.recorded_prompts();
        assert_eq!(prompts.len(), contexts.len());
        let prompt = prompts.join("\n");
        for (case, forbidden) in [
            ("title", TITLE_SECRET),
            ("summary", SUMMARY_SECRET),
            ("user or assistant outline", OUTLINE_SECRET),
            ("system message", SYSTEM_SECRET),
            ("Session topic", TOPIC_SECRET),
            ("split-field label", SPLIT_LABEL),
            ("ordinary tool output", TOOL_MARKER),
            ("topic truncation boundary", BOUNDARY_SECRET_PREFIX),
        ] {
            assert!(
                !prompt.contains(forbidden),
                "private source reached the extraction provider: {case}"
            );
        }
        assert!(prompt.contains(crate::auto_dream_privacy::REDACTED_EXTRACTION_SOURCE));
        assert!(!prompt.contains(OPAQUE_SESSION_ID));
        assert!(prompt.contains("source-session-"));

        let documents = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list memory documents");
        assert_eq!(documents.len(), 2);
        let document_titles = documents
            .iter()
            .map(|document| document.frontmatter.title.as_str())
            .collect::<HashSet<_>>();
        assert!(document_titles.contains("User prefers concise replies"));
        assert!(document_titles.contains("Opaque authority remains attributable"));

        let records = ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list Ledger records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record.title, "Renew passport");
    }

    #[tokio::test]
    async fn extraction_batches_checkpoint_before_sinks_and_resume_without_restatement() {
        const FIRST_RAW_SOURCE: &str = "RAW_FIRST_SOURCE_MUST_NOT_ENTER_CHECKPOINT";
        const SECOND_RAW_SOURCE: &str = "RAW_SECOND_SOURCE_MUST_NOT_ENTER_CHECKPOINT";

        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let mut session = bamboo_agent_core::Session::new("session-batched-retry", "model");
        session.title = "Batched extraction".to_string();
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            FIRST_RAW_SOURCE,
            1,
            32,
        ));
        storage.save_session(&session).await.expect("save session");

        let first_response = extraction_response("First durable fact", "First safe output");
        let failing_provider = Arc::new(ScriptedProvider::new(vec![
            Ok(first_response),
            Err("injected later-batch failure".to_string()),
        ]));
        let provider: Arc<dyn LLMProvider> = failing_provider.clone();
        let memory = MemoryStore::new(temp_dir.path());
        let context = AutoDreamContext {
            session_store: session_store.clone(),
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let mut contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);
        contexts[0].summary = Some(FIRST_RAW_SOURCE.to_string());
        contexts[0].topics.clear();
        let mut second = contexts[0].clone();
        second.summary = Some(SECOND_RAW_SOURCE.to_string());
        contexts.push(second);

        let ledger = LedgerStore::new(temp_dir.path());
        let error = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect_err("a later provider batch must fail the whole transaction");
        assert!(error.contains("injected later-batch failure"));
        assert_eq!(failing_provider.recorded_prompts().len(), 2);
        assert!(memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list memory after provider failure")
            .is_empty());
        assert!(ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list Ledger after provider failure")
            .is_empty());
        assert!(memory
            .read_session_state("session-batched-retry")
            .await
            .expect("read retry state")
            .last_extracted_at
            .is_none());

        let checkpoint_dir = extraction_checkpoint_session_dir(&context, "session-batched-retry");
        let mut entries = tokio::fs::read_dir(&checkpoint_dir)
            .await
            .expect("read checkpoint directory");
        let checkpoint_path = entries
            .next_entry()
            .await
            .expect("read checkpoint entry")
            .expect("one completed batch checkpoint")
            .path();
        assert!(entries
            .next_entry()
            .await
            .expect("read end of checkpoint directory")
            .is_none());
        let checkpoint = tokio::fs::read_to_string(&checkpoint_path)
            .await
            .expect("read candidate-only checkpoint");
        assert!(checkpoint.contains("First safe output"));
        assert!(!checkpoint.contains(FIRST_RAW_SOURCE));
        assert!(!checkpoint.contains(SECOND_RAW_SOURCE));
        assert!(!checkpoint.contains(&temp_dir.path().to_string_lossy().to_string()));

        let retry_sequence = Arc::new(SequenceProvider::new(vec![extraction_response(
            "Second durable fact",
            "Second safe output",
        )]));
        let retry_provider: Arc<dyn LLMProvider> = retry_sequence.clone();
        let writes = extract_and_persist_durable_candidates(
            &context,
            &retry_provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("retry should reuse the completed first batch");
        assert_eq!(writes.memory, 2);
        assert_eq!(retry_sequence.recorded_prompts().len(), 1);
        assert!(retry_sequence.recorded_prompts()[0].contains(SECOND_RAW_SOURCE));
        assert!(!retry_sequence.recorded_prompts()[0].contains(FIRST_RAW_SOURCE));
        assert_eq!(
            memory
                .list_memory_documents(MemoryScope::Global, None)
                .await
                .expect("list completed extraction")
                .len(),
            2
        );
        assert_eq!(
            memory
                .read_session_state("session-batched-retry")
                .await
                .expect("read completed watermark")
                .last_extracted_at
                .as_deref(),
            Some(contexts[0].entry.updated_at.to_rfc3339().as_str())
        );
        let mut remaining = tokio::fs::read_dir(&checkpoint_dir)
            .await
            .expect("read cleaned checkpoint directory");
        assert!(remaining
            .next_entry()
            .await
            .expect("read cleaned checkpoint directory")
            .is_none());
    }

    #[tokio::test]
    async fn extraction_checkpoint_io_revalidates_the_privacy_boundary() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let checkpoint_id = "a".repeat(64);
        let checkpoint_path = temp_dir.path().join(format!("{checkpoint_id}.json"));
        let checkpoint = ExtractionCheckpoint {
            version: EXTRACTION_CHECKPOINT_VERSION,
            batch_id: checkpoint_id.clone(),
            session_key: extraction_checkpoint_session_key("session-checkpoint-privacy"),
            source_updated_at: "2026-09-20T00:00:20Z".to_string(),
            transaction_id: "b".repeat(64),
            batch_index: 0,
            batch_count: 1,
            topics_fingerprint: None,
            extracted: ExtractedCandidateBatch {
                memory: vec![DurableExtractionCandidate {
                    title: "Credential".to_string(),
                    kind: "reference".to_string(),
                    content: "OPENAI_API_KEY=sk-proj-12345678901234567890".to_string(),
                    scope: Some("global".to_string()),
                    tags: Vec::new(),
                    session_id: Some("session-checkpoint-privacy".to_string()),
                    confidence: Some("high".to_string()),
                }],
                ledger: Vec::new(),
            },
        };

        let write_error = write_extraction_checkpoint(&checkpoint_path, &checkpoint)
            .await
            .expect_err("unsafe candidates must not be checkpointed");
        assert!(write_error.contains("privacy boundary"));
        assert!(!checkpoint_path.exists());

        tokio::fs::write(
            &checkpoint_path,
            serde_json::to_vec_pretty(&checkpoint).expect("serialize tampered checkpoint"),
        )
        .await
        .expect("write tampered checkpoint");
        let read_error = read_extraction_checkpoint(&checkpoint_path, &checkpoint_id)
            .await
            .expect_err("unsafe persisted candidates must not be replayed");
        assert!(read_error.contains("privacy boundary"));
    }

    #[tokio::test]
    async fn sink_failure_replays_checkpoint_without_a_second_provider_call() {
        const RAW_SOURCE: &str = "RAW_SINK_RETRY_SOURCE_MUST_NOT_ENTER_CHECKPOINT";

        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let mut session = bamboo_agent_core::Session::new("session-sink-retry", "model");
        session.title = "Sink retry".to_string();
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            RAW_SOURCE, 1, 32,
        ));
        storage.save_session(&session).await.expect("save session");

        let response = serde_json::json!({
            "candidates": [{
                "title": "Sink retry durable fact",
                "type": "reference",
                "scope": "global",
                "content": "Sanitized durable output",
                "tags": ["retry"],
                "session_id": "source-session-0001",
                "confidence": "high"
            }],
            "ledger_candidates": [{
                "title": "Confirm sink retry",
                "kind": "todo",
                "excerpt": "I will confirm the sink retry.",
                "session_id": "source-session-0001",
                "confidence": "high"
            }],
            "source_exhausted": true
        })
        .to_string();
        let sequence = Arc::new(SequenceProvider::new(vec![response]));
        let provider: Arc<dyn LLMProvider> = sequence.clone();
        let memory = MemoryStore::new(temp_dir.path());
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);

        let invalid_ledger_root = temp_dir.path().join("ledger-root-is-a-file");
        std::fs::write(&invalid_ledger_root, "not a directory").expect("write invalid root");
        let failing_ledger = LedgerStore::new(&invalid_ledger_root);
        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &failing_ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect_err("Ledger sink must fail after the memory write");
        assert_eq!(sequence.recorded_prompts().len(), 1);
        assert_eq!(
            memory
                .list_memory_documents(MemoryScope::Global, None)
                .await
                .expect("list partial memory sink")
                .len(),
            1
        );
        assert!(memory
            .read_session_state("session-sink-retry")
            .await
            .expect("read failed sink watermark")
            .last_extracted_at
            .is_none());

        let no_call_sequence = Arc::new(SequenceProvider::new(Vec::new()));
        let no_call_provider: Arc<dyn LLMProvider> = no_call_sequence.clone();
        let good_ledger = LedgerStore::new(temp_dir.path().join("good-ledger-root"));
        let writes = extract_and_persist_durable_candidates(
            &context,
            &no_call_provider,
            &memory,
            &good_ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("retry should replay the exact checkpoint");
        assert_eq!(writes.memory, 0);
        assert_eq!(writes.ledger, 1);
        assert!(no_call_sequence.recorded_prompts().is_empty());
        assert_eq!(
            good_ledger
                .list_records(LedgerScope::Global, None, &RecordFilter::default())
                .await
                .expect("list replayed Ledger sink")
                .len(),
            1
        );
        assert_eq!(
            memory
                .read_session_state("session-sink-retry")
                .await
                .expect("read replayed watermark")
                .last_extracted_at
                .as_deref(),
            Some(contexts[0].entry.updated_at.to_rfc3339().as_str())
        );
    }

    #[tokio::test]
    async fn saturated_extraction_pages_continue_to_exhaustion_and_reject_no_progress() {
        let first_page_candidates = (0..EXTRACTION_MAX_CANDIDATES)
            .map(|index| {
                serde_json::json!({
                    "title": format!("Fact {index}"),
                    "type": "reference",
                    "scope": "global",
                    "content": format!("Safe fact {index}"),
                    "tags": [],
                    "session_id": "source-session-0001",
                    "confidence": "high"
                })
            })
            .collect::<Vec<_>>();
        let first_page = serde_json::json!({
            "candidates": first_page_candidates,
            "ledger_candidates": [],
            "source_exhausted": false
        })
        .to_string();
        let final_page = extraction_response("Fact 8", "Safe fact 8");
        let sequence = Arc::new(SequenceProvider::new(vec![first_page.clone(), final_page]));
        let provider: Arc<dyn LLMProvider> = sequence.clone();
        let extracted = extract_durable_candidate_batch(
            &provider,
            "fast-model",
            "bounded source".to_string(),
            "session-pagination",
        )
        .await
        .expect("saturated source should continue to exhaustion");
        assert_eq!(extracted.memory.len(), EXTRACTION_MAX_CANDIDATES + 1);
        let prompts = sequence.recorded_prompts();
        assert_eq!(prompts.len(), 2);
        assert!(prompts[1].contains("Exhaustive continuation page 2"));
        assert!(prompts[1].contains("already_returned"));

        let repeated = Arc::new(SequenceProvider::new(vec![first_page.clone(), first_page]));
        let repeated_provider: Arc<dyn LLMProvider> = repeated;
        let error = extract_durable_candidate_batch(
            &repeated_provider,
            "fast-model",
            "bounded source".to_string(),
            "session-pagination",
        )
        .await
        .expect_err("a repeated continuation page must fail closed");
        assert!(error.contains("made no safe, deduplicated progress"));

        let oversized_ledger_page = serde_json::json!({
            "candidates": [],
            "ledger_candidates": (0..=EXTRACTION_MAX_CANDIDATES)
                .map(|index| serde_json::json!({
                    "title": format!("Commitment {index}"),
                    "kind": "todo",
                    "excerpt": format!("I will complete commitment {index}."),
                    "session_id": "source-session-0001",
                    "confidence": "high"
                }))
                .collect::<Vec<_>>(),
            "source_exhausted": true
        })
        .to_string();
        let oversized = Arc::new(SequenceProvider::new(vec![oversized_ledger_page]));
        let oversized_provider: Arc<dyn LLMProvider> = oversized;
        let error = extract_durable_candidate_batch(
            &oversized_provider,
            "fast-model",
            "bounded source".to_string(),
            "session-pagination",
        )
        .await
        .expect_err("Ledger candidates share the per-page safety cap");
        assert!(error.contains("Ledger candidates; maximum is 8"));
    }

    #[tokio::test]
    async fn extract_and_persist_durable_candidates_writes_memory() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let extraction_response = serde_json::json!({
            "candidates": [
                {
                    "title": "User prefers terse responses",
                    "type": "feedback",
                    "scope": "project",
                    "content": "The user prefers terse responses and no recap.",
                    "tags": ["preference", "style"],
                    "session_id": "source-session-0001",
                    "confidence": "high"
                },
                {
                    "title": "x".repeat(MAX_MEMORY_TITLE_LEN + 1),
                    "type": "feedback",
                    "scope": "global",
                    "content": "Invalid model output must be rejected before it can split a batch.",
                    "session_id": "session-auto"
                },
                {
                    "title": "Hallucinated source",
                    "type": "feedback",
                    "scope": "global",
                    "content": "An unknown source session must never be persisted.",
                    "session_id": "session-hallucinated"
                }
            ],
            "ledger_candidates": [
                {
                    "title": "Review terse response preference",
                    "kind": "todo",
                    "excerpt": "Confirm the stable response preference.",
                    "session_id": "source-session-0001",
                    "confidence": "high"
                }
            ]
        })
        .to_string();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            extraction_response.clone(),
            extraction_response,
        ]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

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
            memory: memory.clone(),
            provider: provider.clone(),
            config: config.clone(),
            provider_registry: test_registry(),
        };
        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);
        let extracted_source_updated_at = contexts[0].entry.updated_at;

        // Simulate a new turn arriving after the extraction input was captured
        // but before the model call completed. The marker must retain the older
        // source watermark so this newer content remains eligible next time.
        session.updated_at = extracted_source_updated_at + chrono::Duration::seconds(1);
        session.add_message(Message::user("One newer turn arrived during extraction."));
        session.updated_at = extracted_source_updated_at + chrono::Duration::seconds(1);
        storage
            .save_session(&session)
            .await
            .expect("save concurrent session update");

        let ledger = LedgerStore::new(temp_dir.path());
        let writes = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("extraction should succeed");
        assert_eq!(writes.memory, 1);
        assert_eq!(writes.ledger, 1);
        let documents = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list aliased memory candidate");
        assert_eq!(documents.len(), 1);
        assert!(documents[0]
            .frontmatter
            .sources
            .iter()
            .any(|source| source.kind == "session" && source.id == "session-auto"));
        let records = ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list aliased Ledger candidate");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].record.source.session_id.as_deref(),
            Some("session-auto")
        );
        let extraction_state = memory
            .read_session_state("session-auto")
            .await
            .expect("read extraction source watermark");
        assert_eq!(
            extraction_state.last_extracted_at.as_deref(),
            Some(extracted_source_updated_at.to_rfc3339().as_str())
        );
        let newer_contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(
            newer_contexts.len(),
            1,
            "a session update newer than the captured extraction watermark must remain eligible"
        );

        let replay = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("a post-write retry should be idempotent");
        assert_eq!(
            replay.memory, 0,
            "an exact candidate already committed before a later batch failure must not duplicate"
        );
        assert_eq!(replay.ledger, 0);

        let results = memory
            .query_scope(
                MemoryScope::Global,
                None,
                Some("terse recap"),
                None,
                None,
                None,
                &bamboo_memory::memory_store::MemoryQueryOptions {
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
    }

    #[tokio::test]
    async fn auto_dream_does_not_write_candidates_from_malformed_project_session() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let workspace = temp_dir.path().join("workspace-malformed");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let project_id = ProjectId::parse("project-auto-dream-unused").expect("project id");
        let project_home = temp_dir.path().join("projects").join(project_id.as_str());
        let resolver = ProjectContextResolver::new(Arc::new(StaticProjectSource(
            crate::project_context::ProjectDescriptor {
                id: project_id.clone(),
                name: "Unused".to_string(),
                project_path: Some(workspace.clone()),
                home: project_home.clone(),
                workspace_bindings: Vec::new(),
                resources: ProjectResourceSummary {
                    project_id: project_id.clone(),
                    resource_revision: 1,
                    resources: Vec::new(),
                },
            },
        )));
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"Must not persist\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"MALFORMED PROJECT SESSION MUST NOT WRITE\",\"tags\":[\"secret\"],\"session_id\":\"session-malformed-auto-dream\"}]}".to_string(),
        ]));
        let context = AutoDreamContext {
            session_store,
            storage: storage.clone(),
            memory: MemoryStore::new(temp_dir.path()),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(config_with_memory(
                bamboo_config::MemoryConfig {
                    background_model: Some("fast-model".to_string()),
                    auto_dream_enabled: true,
                    ..bamboo_config::MemoryConfig::default()
                },
            ))),
            provider_registry: test_registry(),
        };
        let mut session = bamboo_agent_core::Session::new("session-malformed-auto-dream", "model");
        session.set_project_id_meta("../malformed".to_string());
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Sensitive malformed session context.",
            2,
            80,
        ));
        session.add_message(Message::user("Remember this."));
        storage.save_session(&session).await.expect("save session");
        let memory = MemoryStore::new(temp_dir.path());
        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);

        let writes = extract_and_persist_durable_candidates_with_project_resolver(
            &context,
            &provider,
            &memory,
            &LedgerStore::new(temp_dir.path()),
            "fast-model",
            &contexts,
            Some(&resolver),
            false,
        )
        .await
        .expect("malformed candidate should be skipped");
        assert_eq!(writes, ExtractionWrites::default());
        let global_count = memory
            .count_scope_memories(MemoryScope::Global, None)
            .await
            .expect("count global memories");
        assert_eq!(global_count, 0);
        let project_count = memory
            .for_project(&project_id)
            .count_scope_memories(MemoryScope::Project, Some(project_id.as_str()))
            .await
            .expect("count Project memories");
        assert_eq!(project_count, 0);
    }

    #[tokio::test]
    async fn assigned_project_extraction_uses_project_home_across_workspace_switches() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let workspace_one = temp_dir.path().join("workspace-one");
        let workspace_two = temp_dir.path().join("workspace-two");
        std::fs::create_dir_all(&workspace_one).expect("workspace one");
        std::fs::create_dir_all(&workspace_two).expect("workspace two");
        let project_id =
            ProjectId::parse("sk-proj-abcdefghijklmnop").expect("secret-like project id");
        let project_home = temp_dir.path().join("projects").join(project_id.as_str());
        let memory_root = project_home.join("memory/v1");
        let resolver = ProjectContextResolver::new(Arc::new(StaticProjectSource(
            crate::project_context::ProjectDescriptor {
                id: project_id.clone(),
                name: "Auto Dream".to_string(),
                project_path: Some(workspace_one.clone()),
                home: project_home.clone(),
                workspace_bindings: vec![
                    WorkspaceBinding {
                        path: workspace_one.to_string_lossy().into_owned(),
                        label: None,
                        git_common_dir: None,
                    },
                    WorkspaceBinding {
                        path: workspace_two.to_string_lossy().into_owned(),
                        label: None,
                        git_common_dir: None,
                    },
                ],
                resources: ProjectResourceSummary {
                    project_id: project_id.clone(),
                    resource_revision: 1,
                    resources: Vec::new(),
                },
            },
        )));
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let sequence = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"First Project fact\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"The first stable Project fact.\",\"tags\":[\"project\"],\"session_id\":\"source-session-0001\"}]}".to_string(),
            "{\"candidates\":[{\"title\":\"Second Project fact\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"The second stable Project fact after switching workspaces.\",\"tags\":[\"project\"],\"session_id\":\"source-session-0001\"}]}".to_string(),
        ]));
        let provider: Arc<dyn LLMProvider> = sequence.clone();
        let context = AutoDreamContext {
            session_store,
            storage: storage.clone(),
            memory: MemoryStore::new(temp_dir.path()),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(config_with_memory(
                bamboo_config::MemoryConfig {
                    background_model: Some("fast-model".to_string()),
                    auto_dream_enabled: true,
                    ..bamboo_config::MemoryConfig::default()
                },
            ))),
            provider_registry: test_registry(),
        };
        let base_memory = MemoryStore::new(temp_dir.path());
        let ledger = LedgerStore::new(temp_dir.path());
        let mut session = bamboo_agent_core::Session::new("session-assigned", "model");
        session.set_project_id_meta(project_id.to_string());
        session.set_workspace_path_meta(workspace_one.to_string_lossy().into_owned());
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Stable Project facts.",
            2,
            80,
        ));
        session.add_message(Message::user("Remember this for the Project."));
        storage.save_session(&session).await.expect("save session");
        base_memory
            .write_session_topic("session-assigned", "default", "Project fact source.")
            .await
            .expect("write session topic");

        for (index, workspace) in [&workspace_one, &workspace_two].into_iter().enumerate() {
            session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
            if index > 0 {
                session.add_message(Message::user(
                    "A second stable Project fact arrived after the workspace switch.",
                ));
            }
            storage
                .save_session(&session)
                .await
                .expect("save switched session");
            let contexts = collect_candidate_session_contexts(
                &context,
                &base_memory,
                Utc::now() - chrono::Duration::hours(24),
            )
            .await;
            let writes = extract_and_persist_durable_candidates_with_project_resolver(
                &context,
                &provider,
                &base_memory,
                &ledger,
                "fast-model",
                &contexts,
                Some(&resolver),
                false,
            )
            .await
            .expect("Project extraction");
            assert_eq!(writes.memory, 1);
        }

        let project_memory = base_memory.for_project(&project_id);
        let results = project_memory
            .query_scope(
                MemoryScope::Project,
                Some(project_id.as_str()),
                Some("Project fact"),
                None,
                None,
                None,
                &bamboo_memory::memory_store::MemoryQueryOptions::default(),
            )
            .await
            .expect("query Project memory");
        assert_eq!(results.matched_count, 2);
        assert!(memory_root.join("topics").is_dir());
        let prompts = sequence.recorded_prompts();
        assert_eq!(prompts.len(), 2);
        for prompt in prompts {
            assert!(!prompt.contains(project_id.as_str()));
            assert!(prompt.contains("source-project-0001"));
        }
    }

    #[tokio::test]
    async fn extract_and_persist_durable_candidates_ignores_empty_candidate_lists() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
        ]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

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
            memory: memory.clone(),
            provider,
            config,
            provider_registry: test_registry(),
        };
        let sessions = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        let ledger = LedgerStore::new(temp_dir.path());
        let writes = extract_and_persist_durable_candidates(
            &context,
            &context.provider,
            &memory,
            &ledger,
            "fast-model",
            &sessions,
        )
        .await
        .expect("empty extraction should succeed");
        assert_eq!(writes, ExtractionWrites::default());
        let state = memory
            .read_session_state("session-empty")
            .await
            .expect("read empty extraction watermark");
        assert!(state.last_extracted_at.is_some());
        let remaining = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert!(
            remaining.is_empty(),
            "a successful empty extraction must not spend another model call on unchanged input"
        );
    }

    #[tokio::test]
    async fn extraction_rejects_unknown_envelope_fields_before_sinks_and_watermark() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            r#"{"credential_label":"password","candidates":[{"title":"Production database","type":"reference","content":"hunter2","session_id":"session-unknown-field"}]}"#
                .to_string(),
        ]));

        let mut session = bamboo_agent_core::Session::new("session-unknown-field", "model");
        session.add_message(Message::user("Remember the production database."));
        storage.save_session(&session).await.expect("save session");
        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic(
                "session-unknown-field",
                "database",
                "Production database details",
            )
            .await
            .expect("write topic");
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let sessions = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        let ledger = LedgerStore::new(temp_dir.path());

        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &sessions,
        )
        .await
        .expect_err("unknown envelope fields must fail the extraction");
        assert!(memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list memory")
            .is_empty());
        assert!(ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list Ledger")
            .is_empty());
        assert!(
            memory
                .read_session_state("session-unknown-field")
                .await
                .expect("read extraction state")
                .last_extracted_at
                .is_none(),
            "a rejected provider payload must not acknowledge its source"
        );
    }

    #[tokio::test]
    async fn run_auto_dream_once_updates_dream_and_persists_candidates() {
        const TITLE_SECRET: &str = "API key: hunter2";
        const SUMMARY_SECRET: &str = "private key: hunter2";
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"User prefers concise answers\",\"type\":\"feedback\",\"scope\":\"project\",\"content\":\"The user prefers concise answers and minimal recap.\",\"tags\":[\"preference\"],\"session_id\":\"source-session-0001\"}],\"ledger_candidates\":[{\"title\":\"Renew passport\",\"kind\":\"todo\",\"due_at\":\"2026-08-01T00:00:00Z\",\"starts_at\":null,\"excerpt\":\"I need to renew my passport before August\",\"session_id\":\"source-session-0001\",\"confidence\":\"high\"}]}".to_string(),
            "## Current durable context\n- Durable signal found\n\n## Cross-session patterns\n- Prefer concise answers\n\n## Active threads to remember\n- Memory extraction\n\n## Stable constraints and preferences\n- Terse replies\n\n## Open risks or questions\n- None".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let mut session = bamboo_agent_core::Session::new("session-dream-run", "model");
        session.title = TITLE_SECRET.to_string();
        session.metadata.insert(
            "workspace_path".to_string(),
            temp_dir
                .path()
                .join("workspace-run")
                .to_string_lossy()
                .to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            SUMMARY_SECRET,
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
            memory: memory.clone(),
            provider: provider_handle,
            config,
            provider_registry: test_registry(),
        };
        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("auto dream run should succeed")
            .expect("auto dream should produce output");
        assert_eq!(result.used_model, "fast-model");
        assert_eq!(result.session_count, 1);

        let prompts = provider.recorded_prompts();
        assert_eq!(prompts.len(), 2, "one extraction and one Dream call");
        assert!(
            prompts[0].contains("Extract only durable memory candidates"),
            "the extraction model call must run first"
        );
        assert!(
            prompts[1].contains("User prefers concise answers"),
            "Dream synthesis must re-read canonical MEMORY after extraction"
        );
        for (index, prompt) in prompts.iter().enumerate() {
            assert!(
                !prompt.contains(TITLE_SECRET),
                "private Session title reached AutoDream provider call {index}"
            );
            assert!(
                !prompt.contains(SUMMARY_SECRET),
                "private Session summary reached AutoDream provider call {index}"
            );
            assert!(prompt.contains(crate::auto_dream_privacy::REDACTED_EXTRACTION_SOURCE));
        }

        let dream = read_test_dream(&memory, MemoryScope::Global, None)
            .await
            .expect("dream should exist");
        assert!(dream.contains("Bamboo Dream Notebook"));
        assert!(dream.contains("Durable signal found"));

        let results = memory
            .query_scope(
                MemoryScope::Global,
                None,
                Some("concise answers"),
                None,
                None,
                None,
                &bamboo_memory::memory_store::MemoryQueryOptions {
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

        // The SAME extraction call also proposed a ledger candidate — it must
        // land as a suggested Global record attributed to the extractor.
        let ledger = LedgerStore::new(temp_dir.path());
        let records = ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list ledger records");
        assert_eq!(records.len(), 1);
        let record = &records[0].record;
        assert_eq!(record.title, "Renew passport");
        assert_eq!(record.kind, RecordKind::Todo);
        assert_eq!(record.status, bamboo_domain::ledger::RecordStatus::Open);
        assert_eq!(record.scope, LedgerScope::Global);
        assert_eq!(record.tags, vec!["suggested".to_string()]);
        assert_eq!(record.source.created_by, RecordActor::Extractor);
        assert_eq!(
            record.source.session_id.as_deref(),
            Some("session-dream-run")
        );
        assert_eq!(
            record.source.excerpt.as_deref(),
            Some("I need to renew my passport before August")
        );
        assert_eq!(
            record.time.due_at.map(|at| at.to_rfc3339()),
            Some("2026-08-01T00:00:00+00:00".to_string())
        );
        assert!(
            record.schedule_ids.is_empty(),
            "suggested records must not get schedules"
        );
    }

    #[tokio::test]
    async fn persist_ledger_candidates_writes_suggested_records_and_skips_unusable_ones() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let ledger = LedgerStore::new(temp_dir.path());

        let candidate = |title: &str,
                         kind: &str,
                         due_at: Option<&str>,
                         starts_at: Option<&str>,
                         confidence: Option<&str>| {
            LedgerExtractionCandidate {
                title: title.to_string(),
                kind: kind.to_string(),
                due_at: due_at.map(ToString::to_string),
                starts_at: starts_at.map(ToString::to_string),
                excerpt: Some(format!("The user said: {title}")),
                session_id: Some("session-ledger".to_string()),
                confidence: confidence.map(ToString::to_string),
            }
        };

        let long_title = "x".repeat(MAX_RECORD_TITLE_LEN + 1);
        let candidates = vec![
            candidate(
                "Renew passport",
                "todo",
                Some("2026-08-01T00:00:00Z"),
                None,
                Some("high"),
            ),
            candidate(
                "Dentist appointment",
                "event",
                None,
                Some("2026-07-20T09:00:00+02:00"),
                Some("medium"),
            ),
            // Skipped: low confidence.
            candidate("Maybe buy a boat", "todo", None, None, Some("low")),
            // Skipped: missing confidence.
            candidate("Water the plants", "todo", None, None, None),
            // Skipped: empty title.
            candidate("   ", "todo", None, None, Some("high")),
            // Skipped: title longer than the record title cap.
            candidate(&long_title, "todo", None, None, Some("high")),
            // Skipped: in-batch duplicate (case-insensitive, trimmed).
            candidate("  RENEW PASSPORT  ", "todo", None, None, Some("high")),
            // Written despite malformed timestamps (they parse to None).
            candidate(
                "Call the bank",
                "reminder",
                Some("next week"),
                None,
                Some("medium"),
            ),
        ];

        let writes = persist_ledger_candidates(&ledger, candidates)
            .await
            .expect("persist should succeed");
        assert_eq!(writes, 3);

        let records = ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list records");
        let mut titles: Vec<&str> = records
            .iter()
            .map(|doc| doc.record.title.as_str())
            .collect();
        titles.sort_unstable();
        assert_eq!(
            titles,
            vec!["Call the bank", "Dentist appointment", "Renew passport"]
        );

        for doc in &records {
            assert_eq!(doc.record.status, bamboo_domain::ledger::RecordStatus::Open);
            assert_eq!(doc.record.scope, LedgerScope::Global);
            assert_eq!(doc.record.tags, vec!["suggested".to_string()]);
            assert_eq!(doc.record.source.created_by, RecordActor::Extractor);
            assert_eq!(
                doc.record.source.session_id.as_deref(),
                Some("session-ledger")
            );
            assert!(doc.record.source.excerpt.is_some());
            assert!(doc.record.schedule_ids.is_empty());
        }

        let passport = records
            .iter()
            .find(|doc| doc.record.title == "Renew passport")
            .expect("passport record");
        assert_eq!(passport.record.kind, RecordKind::Todo);
        assert_eq!(
            passport.record.time.due_at.map(|at| at.to_rfc3339()),
            Some("2026-08-01T00:00:00+00:00".to_string())
        );

        let dentist = records
            .iter()
            .find(|doc| doc.record.title == "Dentist appointment")
            .expect("dentist record");
        assert_eq!(dentist.record.kind, RecordKind::Event);
        // Offset timestamps normalize to UTC.
        assert_eq!(
            dentist.record.time.starts_at.map(|at| at.to_rfc3339()),
            Some("2026-07-20T07:00:00+00:00".to_string())
        );

        let bank = records
            .iter()
            .find(|doc| doc.record.title == "Call the bank")
            .expect("bank record");
        assert_eq!(bank.record.kind, RecordKind::Reminder);
        assert!(bank.record.time.due_at.is_none(), "malformed due_at → None");
    }

    #[tokio::test]
    async fn persist_ledger_candidates_dedups_against_existing_open_records() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let ledger = LedgerStore::new(temp_dir.path());

        // Pre-existing OPEN record with the same normalized title → skip.
        ledger
            .write_record(
                LedgerRecord::new(new_record_id(), RecordKind::Todo, "Renew passport"),
                None,
            )
            .await
            .expect("seed existing record");

        let candidates = vec![
            LedgerExtractionCandidate {
                title: "  renew PASSPORT ".to_string(),
                kind: "todo".to_string(),
                excerpt: Some("I need to renew my passport before August".to_string()),
                session_id: Some("session-dup".to_string()),
                confidence: Some("high".to_string()),
                ..LedgerExtractionCandidate::default()
            },
            LedgerExtractionCandidate {
                title: "Book flight to Munich".to_string(),
                kind: "todo".to_string(),
                excerpt: Some("I still have to book my flight to Munich".to_string()),
                session_id: Some("session-dup".to_string()),
                confidence: Some("high".to_string()),
                ..LedgerExtractionCandidate::default()
            },
        ];

        let writes = persist_ledger_candidates(&ledger, candidates)
            .await
            .expect("persist should succeed");
        assert_eq!(
            writes, 1,
            "duplicate of existing open record must be skipped"
        );

        let records = ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list records");
        assert_eq!(records.len(), 2);
        assert!(records
            .iter()
            .any(|doc| doc.record.title == "Book flight to Munich"));
        assert_eq!(
            records
                .iter()
                .filter(|doc| doc.record.title.eq_ignore_ascii_case("renew passport"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn run_project_auto_dream_once_filters_sessions_by_project_and_writes_project_dream() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace_a = temp_dir.path().join("workspace-a");
        let workspace_b = temp_dir.path().join("workspace-b");
        std::fs::create_dir_all(&workspace_a).expect("workspace a");
        std::fs::create_dir_all(&workspace_b).expect("workspace b");
        let project_id_a = ProjectId::parse("project-auto-dream-a").expect("project id");
        let project_key_a = project_id_a.to_string();

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"Project A prefers concise planning\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"Project A plans should stay concise and scoped.\",\"tags\":[\"planning\"],\"session_id\":\"source-session-0001\"}]}".to_string(),
            "## Current durable context\n- Project A signal only\n\n## Cross-session patterns\n- Focus on project A\n\n## Active threads to remember\n- Ship project A\n\n## Stable constraints and preferences\n- Keep scope isolated\n\n## Open risks or questions\n- None".to_string(),
        ]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let mut session_a = bamboo_agent_core::Session::new("session-project-a", "model");
        session_a.title = "Project A session".to_string();
        session_a.set_project_id_meta(project_id_a.to_string());
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

        let base_memory = MemoryStore::new(temp_dir.path());
        base_memory
            .write_session_topic(
                "session-project-a",
                "default",
                "Project A planning should remain concise.",
            )
            .await
            .expect("write session topic a");
        base_memory
            .write_session_topic(
                "session-project-b",
                "default",
                "Project B note that should not be included.",
            )
            .await
            .expect("write session topic b");
        let memory = base_memory.for_project(&project_id_a);

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider,
            config,
            provider_registry: test_registry(),
        };
        let result = run_project_auto_dream_once_for_project(&context, &project_id_a)
            .await
            .expect("project auto dream should succeed")
            .expect("project auto dream should produce output");
        assert_eq!(result.used_model, "fast-model");
        assert_eq!(result.session_count, 1);

        let project_dream = read_test_dream(&memory, MemoryScope::Project, Some(&project_key_a))
            .await
            .expect("project dream should exist");
        assert!(project_dream.contains("Bamboo Dream Notebook"));
        assert!(project_dream.contains("Project key: "));
        assert!(project_dream.contains(&project_key_a));
        assert!(project_dream.contains("Project A signal only"));
        assert!(!project_dream.contains("unrelated project B"));

        let global_dream = read_test_dream(&memory, MemoryScope::Global, None).await;
        assert!(global_dream.is_none());

        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key_a),
                Some("concise planning"),
                None,
                None,
                None,
                &bamboo_memory::memory_store::MemoryQueryOptions {
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
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace_other = temp_dir.path().join("workspace-other");
        let workspace_target = temp_dir.path().join("workspace-target");
        std::fs::create_dir_all(&workspace_other).expect("workspace other");
        std::fs::create_dir_all(&workspace_target).expect("workspace target");
        let target_project_id = ProjectId::parse("project-auto-dream-target").expect("project id");
        let target_project_key = target_project_id.to_string();

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

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

        let memory = MemoryStore::new(temp_dir.path()).for_project(&target_project_id);
        publish_test_dream(
            &memory,
            MemoryScope::Project,
            Some(&target_project_key),
            "# Bamboo Dream Notebook\n\nExisting target project dream",
        )
        .await;

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider,
            config,
            provider_registry: test_registry(),
        };
        let result = run_project_auto_dream_once_for_project(&context, &target_project_id)
            .await
            .expect("project auto dream without sessions should not error");
        assert!(result.is_none());

        let project_dream =
            read_test_dream(&memory, MemoryScope::Project, Some(&target_project_key))
                .await
                .expect("existing dream should remain");
        assert!(project_dream.contains("Existing target project dream"));
    }

    #[tokio::test]
    async fn run_project_auto_dream_once_still_runs_when_auto_background_dream_is_disabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace = temp_dir.path().join("workspace-manual-project-dream");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_id = ProjectId::parse("project-manual-dream").expect("project id");
        let project_key = project_id.to_string();

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
            "## Current durable context\n- Manual project dream worked\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
        ]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let mut session = bamboo_agent_core::Session::new("session-manual-project-dream", "model");
        session.title = "Manual project dream session".to_string();
        session.set_project_id_meta(project_id.to_string());
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

        let base_memory = MemoryStore::new(temp_dir.path());
        base_memory
            .write_session_topic(
                "session-manual-project-dream",
                "default",
                "Manual project dream note.",
            )
            .await
            .expect("write session topic");
        let memory = base_memory.for_project(&project_id);

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider,
            config,
            provider_registry: test_registry(),
        };
        let result = run_project_auto_dream_once_for_project(&context, &project_id)
            .await
            .expect(
                "manual project dream should succeed even when auto background dream is disabled",
            )
            .expect("manual project dream should produce output");
        assert_eq!(result.session_count, 1);

        let project_dream = read_test_dream(&memory, MemoryScope::Project, Some(&project_key))
            .await
            .expect("project dream should exist");
        assert!(project_dream.contains("Manual project dream worked"));
    }

    /// L3: even on a NON-forced pass, once durable memory exists the notebook is
    /// (re)built grounded in the canonical durable memory index — NOT rewritten from
    /// its own prior prose (the retired Refine mode). Also asserts a non-forced pass
    /// does not stamp the periodic-rebuild marker, so the timer still advances.
    #[tokio::test]
    async fn run_auto_dream_once_grounds_notebook_in_durable_index_not_prior_prose() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
            "## Current durable context\n- Grounded in durable memory\n\n## Cross-session patterns\n- Keep continuity\n\n## Active threads to remember\n- Refresh blockers\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let workspace = temp_dir.path().join("workspace-grounded-mode");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_id = ProjectId::parse("project-grounded-dream").expect("project id");
        let project_key = project_id.to_string();

        let mut session = bamboo_agent_core::Session::new("session-grounded-mode", "model");
        session.title = "Grounded mode test".to_string();
        session.set_project_id_meta(project_id.to_string());
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "Recent session summary for grounded mode.",
            3,
            120,
        ));
        session.add_message(Message::user("Update the dream from durable memory."));
        storage.save_session(&session).await.expect("save session");

        let memory = MemoryStore::new(temp_dir.path()).for_project(&project_id);
        // Existing notebook with only a "Last consolidated at" line (NO "Last full
        // rebuild at") → force_full_rebuild is false, so this is a NON-forced pass.
        publish_test_dream(
            &memory,
            MemoryScope::Project,
            Some(&project_key),
            "# Bamboo Dream Notebook\n\nProject key: project\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Stale prior notebook prose that must NOT drive the rebuild\n",
        )
        .await;
        memory
            .write_memory(
                MemoryScope::Project,
                Some(&project_key),
                bamboo_memory::memory_store::DurableMemoryType::Project,
                "Canonical release decision",
                "Release freeze starts Tuesday and all mobile changes require review.",
                &["release".to_string(), "mobile".to_string()],
                Some("session-grounded-mode"),
                "main-model",
                false,
                None,
            )
            .await
            .expect("write project durable memory");

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider_handle,
            config,
            provider_registry: test_registry(),
        };

        let result = run_project_auto_dream_once_for_project(&context, &project_id)
            .await
            .expect("grounded auto dream should succeed")
            .expect("dream output should be produced");
        assert_eq!(result.session_count, 1);

        let prompts = provider.recorded_prompts();
        assert!(prompts.len() >= 2);
        // Grounded in the durable memory index, not the prior notebook prose.
        assert!(prompts[1].contains("## Durable memory index"));
        assert!(prompts[1].contains("Canonical release decision"));
        assert!(prompts[1].contains("canonical durable memory plus recent session activity"));
        assert!(
            !prompts[1].contains("## Existing Dream notebook"),
            "notebook must not be rewritten from its own prior prose (Refine retired)"
        );
        assert!(!prompts[1].contains("Stale prior notebook prose"));

        // The first grounded Rebuild (no prior marker) BOOTSTRAPS the periodic
        // full-rebuild marker so the 30-day cadence has a start point (#261); it
        // is only SUBSEQUENT non-forced passes that preserve it without resetting.
        let dream = read_test_dream(&memory, MemoryScope::Project, Some(&project_key))
            .await
            .expect("project dream should exist");
        assert!(dream.contains("Grounded in durable memory"));
        assert!(
            dream.contains("Last full rebuild at:"),
            "the first grounded Rebuild must bootstrap the full-rebuild marker (#261)"
        );
    }

    #[tokio::test]
    async fn run_auto_dream_once_forces_periodic_full_rebuild_using_memory_index() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
            "## Current durable context\n- Rebuilt from durable memory index\n\n## Cross-session patterns\n- Canonical project history\n\n## Active threads to remember\n- Refresh active blockers\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let workspace = temp_dir.path().join("workspace-rebuild-mode");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        let project_id = ProjectId::parse("project-rebuild-dream").expect("project id");
        let project_key = project_id.to_string();

        let mut session = bamboo_agent_core::Session::new("session-rebuild-mode", "model");
        session.title = "Rebuild mode test".to_string();
        session.set_project_id_meta(project_id.to_string());
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

        let memory = MemoryStore::new(temp_dir.path()).for_project(&project_id);
        publish_test_dream(
            &memory,
            MemoryScope::Project,
            Some(&project_key),
            "# Bamboo Dream Notebook\n\nProject key: project\nLast consolidated at: 2026-02-02T16:00:00Z\nLast full rebuild at: 2026-02-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing project dream\n",
        )
        .await;
        memory
            .write_memory(
                MemoryScope::Project,
                Some(&project_key),
                bamboo_memory::memory_store::DurableMemoryType::Project,
                "Canonical release decision",
                "Release freeze starts Tuesday and all mobile changes require review.",
                &["release".to_string(), "mobile".to_string()],
                Some("session-rebuild-mode"),
                "main-model",
                false,
                None,
            )
            .await
            .expect("write project durable memory");

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider_handle,
            config,
            provider_registry: test_registry(),
        };

        let result = run_project_auto_dream_once_for_project(&context, &project_id)
            .await
            .expect("rebuild auto dream should succeed")
            .expect("rebuild dream output should be produced");
        assert_eq!(result.session_count, 1);

        let prompts = provider.recorded_prompts();
        assert!(prompts.len() >= 2);
        assert!(prompts[1].contains("## Durable memory index"));
        assert!(prompts[1].contains("Canonical release decision"));
        assert!(prompts[1].contains("canonical durable memory plus recent session activity"));

        let dream = read_test_dream(&memory, MemoryScope::Project, Some(&project_key))
            .await
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
    async fn run_auto_dream_once_normalizes_nested_notebook_output() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "{\"candidates\":[]}".to_string(),
            "```md\n# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-10T06:28:54.680302+00:00\nSessions reviewed: 2\nModel: gpt-5-mini\n\n## Current durable context\n- Refined durable theme\n\n## Cross-session patterns\n- Keep continuity\n\n## Active threads to remember\n- Update the notebook\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None\n```".to_string(),
        ]);
        let provider_handle: Arc<dyn LLMProvider> = Arc::new(provider.clone());
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

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
        publish_test_dream(
            &memory,
            MemoryScope::Global,
            None,
            "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\nSessions reviewed: 2\nModel: fast-model\n\n## Current durable context\n- Existing durable thread\n",
        )
        .await;
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
            memory: memory.clone(),
            provider: provider_handle,
            config,
            provider_registry: test_registry(),
        };

        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("refine normalize auto dream should succeed")
            .expect("dream output should be produced");
        assert_eq!(result.session_count, 1);

        let dream = read_test_dream(&memory, MemoryScope::Global, None)
            .await
            .expect("dream should exist");
        assert!(dream.contains("Refined durable theme"));
        assert!(!dream.contains("```md"));
        assert_eq!(dream.matches("# Bamboo Dream Notebook").count(), 1);
    }

    #[tokio::test]
    async fn run_auto_dream_once_retries_dream_without_repeating_durable_extraction() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let memory = MemoryStore::new(temp_dir.path());
        let old_content = "# Bamboo Dream Notebook\n\nLast consolidated at: 2026-04-02T16:00:00Z\n\n## Current durable context\n- Complete old orientation";
        publish_test_dream(&memory, MemoryScope::Global, None, old_content).await;

        let mut session = bamboo_agent_core::Session::new("session-cas-dream", "model");
        session.title = "Dream CAS test".to_string();
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "A recent session that should trigger one Dream run.",
            2,
            80,
        ));
        session.add_message(Message::user("Refresh durable orientation."));
        storage.save_session(&session).await.expect("save session");
        memory
            .write_session_topic(
                "session-cas-dream",
                "default",
                "Recent context for the CAS test.",
            )
            .await
            .expect("write session topic");

        let calls = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn LLMProvider> = Arc::new(CasMutatingProvider {
            responses: Arc::new(Mutex::new(vec![
                "{\"candidates\":[{\"title\":\"Persist once across CAS retry\",\"type\":\"feedback\",\"scope\":\"global\",\"content\":\"This durable fact must not be duplicated when Dream publication retries.\",\"tags\":[\"cas\"],\"session_id\":\"source-session-0001\"}]}".to_string(),
                "## Current durable context\n- Replacement that must not publish\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
                "## Current durable context\n- Replacement published by the next periodic run\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- None\n\n## Open risks or questions\n- None".to_string(),
            ])),
            calls: calls.clone(),
            memory: memory.clone(),
        });
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider,
            config: Arc::new(RwLock::new(config_with_memory(
                bamboo_config::MemoryConfig {
                    background_model: Some("fast-model".to_string()),
                    auto_dream_enabled: true,
                    ..bamboo_config::MemoryConfig::default()
                },
            ))),
            provider_registry: test_registry(),
        };

        let error = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect_err("a concurrent canonical write must reject Dream publication");
        assert!(error.contains("stale Dream source_generation"), "{error}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "stale CAS must not trigger extraction or synthesis retry"
        );

        let read = memory
            .read_dream_snapshot(MemoryScope::Global, None)
            .await
            .expect("read preserved Dream snapshot");
        assert!(read.stale, "the old snapshot should now report stale");
        assert_eq!(
            read.snapshot.expect("old snapshot must remain").content,
            old_content
        );

        let retry = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("the next periodic run should retry Dream synthesis")
            .expect("the retry should publish a fresh Dream snapshot");
        assert_eq!(retry.session_count, 1);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "the retry must skip the completed extraction call and synthesize once"
        );

        let read = memory
            .read_dream_snapshot(MemoryScope::Global, None)
            .await
            .expect("read retried Dream snapshot");
        assert!(
            !read.stale,
            "the retry should publish against current memory"
        );
        assert!(read
            .snapshot
            .expect("retried snapshot must exist")
            .content
            .contains("Replacement published by the next periodic run"));
        let documents = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list durable memories after retry");
        assert_eq!(
            documents.len(),
            2,
            "one extraction plus one concurrent write"
        );
        assert_eq!(
            documents
                .iter()
                .filter(|document| document.frontmatter.title == "Persist once across CAS retry")
                .count(),
            1,
            "the durable extraction must not repeat when only Dream publication retries"
        );
    }

    #[tokio::test]
    async fn run_auto_dream_once_returns_none_when_disabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        // auto_dream is ON by default (L4), so disable it explicitly to keep
        // covering the disabled gate (not merely "no candidate sessions").
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: false,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp_dir.path()),
            provider,
            config,
            provider_registry: test_registry(),
        };
        let result = run_auto_dream_once(&context)
            .await
            .expect("disabled auto dream should not error");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn run_auto_dream_once_returns_none_without_candidate_sessions() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![]));
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                auto_dream_enabled: true,
                ..bamboo_config::MemoryConfig::default()
            },
        )));

        let context = AutoDreamContext {
            session_store,
            storage,
            memory: MemoryStore::new(temp_dir.path()),
            provider,
            config,
            provider_registry: test_registry(),
        };
        let result = run_auto_dream_once(&context)
            .await
            .expect("no candidate sessions should not error");
        assert!(result.is_none());
    }
}
