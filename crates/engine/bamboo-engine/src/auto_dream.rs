use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use bamboo_agent_core::{Message, Role, Session, SessionKind};
use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordActor, RecordKind, RecordStatus};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::CompressionEventKind;
use bamboo_llm::Config;
use bamboo_llm::{LLMChunk, LLMProvider, LLMRequestOptions};
use bamboo_llm::{ProviderModelRouter, ProviderRegistry};
use bamboo_memory::auto_dream::{
    build_consolidation_prompt, build_extraction_prompt, build_rebuild_consolidation_prompt,
    derive_session_outline, normalize_dream_notebook_body, parse_candidate_scope,
    parse_candidate_type, parse_extraction_candidates, parse_last_consolidated_at,
    parse_last_full_rebuild_at, parse_ledger_candidates, should_force_full_rebuild,
    strip_json_fence, truncate_chars, ConsolidationSessionInfo, DreamCandidateInfo,
    DreamGenerationMode, DurableExtractionCandidate, LedgerExtractionCandidate,
};
use bamboo_memory::ledger_store::store::new_record_id;
use bamboo_memory::ledger_store::{LedgerStore, RecordFilter, MAX_RECORD_TITLE_LEN};
use bamboo_memory::memory_store::{
    DurableMemoryDocument, DurableMemoryStatus, DurableMemoryType, MemoryScope, MemoryStore,
    MAX_MEMORY_TITLE_LEN,
};
use bamboo_storage::{
    search_index::session_history_search_artifact_ids, SessionIndexEntry, SessionStoreV2,
};

use crate::memory_maintenance_fence::acquire_memory_maintenance_fence;
use crate::project_context::ProjectContextResolver;

const DREAM_RUNTIME_SESSION_ID: &str = "__dream__";
const DREAM_TRACING_TARGET: &str = "bamboo.auto_dream";
// Auto-Dream tick cadence now lives in `MemoryConfig::auto_dream_interval_secs`
// (default 30 min); see `spawn_auto_dream_task`.
const DREAM_FULL_REBUILD_INTERVAL_SECS: i64 = 60 * 60 * 24 * 30;
const DREAM_MAX_SESSIONS: usize = 12;
const DREAM_MAX_SUMMARY_CHARS: usize = 12_000;
const EXTRACTION_MAX_CANDIDATES: usize = 8;
const EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH: usize = 32;
const EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH: usize =
    EXTRACTION_MAX_CANDIDATES * EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH;
// Keep source cardinality bounded as the first capacity guard. A single item
// may still contain more than eight atomic facts, so saturated responses use
// exhaustive continuation pages before their source watermark can move.
const RETRIEVAL_EXTRACTION_MAX_SOURCE_ITEMS: usize = EXTRACTION_MAX_CANDIDATES;
const RETRIEVAL_EXTRACTION_MAX_CHARS: usize = 12_000;
const RETRIEVAL_EXTRACTION_CONTENT_SEGMENT_CHARS: usize = 1_500;
const RETRIEVAL_EXTRACTION_CONTINUATION_OVERLAP_CHARS: usize = 128;
const RETRIEVAL_EXTRACTION_HEADER_RESERVE_CHARS: usize = 1_536;
const EXTRACTION_MAX_TOPICS_PER_SESSION: usize = 4;
const EXTRACTION_MAX_TOPIC_CHARS: usize = 1_500;
const EXTRACTION_CHECKPOINT_VERSION: u32 = 2;
const EXTRACTION_CHECKPOINT_DIR: &str = "auto_dream/extraction-checkpoints/v2";
const RETRIEVAL_SOURCE_STATE_VERSION: u32 = 2;
const HISTORY_REWRITE_STATE_VERSION: u32 = 1;
const HISTORY_REWRITE_PLAN_VERSION: u32 = 3;
const HISTORY_REWRITE_MAX_PRESERVATION_SESSIONS: usize = 64;
const AUTO_DREAM_MEMORY_ACTOR: &str = "background-fast-model";
const GARDENER_MEMORY_ACTORS: [&str; 2] = ["memory-gardener", "memory-dedup-gardener"];
const REDACTED_EXTRACTION_SOURCE: &str =
    "[sensitive content omitted before durable-memory extraction]";

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
    retrieval_source_key: Option<String>,
    history_revision: Option<String>,
    /// A mixed-lineage gardener document can require exact evidence from a
    /// second Session while the transaction and acknowledgement still belong
    /// to the rewritten Session. Ordinary extraction leaves both fields empty.
    transaction_owner_session_id: Option<String>,
    transaction_source_updated_at: Option<String>,
}

#[derive(Debug, Clone)]
struct DreamSourceWindow {
    existing_dream: Option<String>,
    durable_memory_index: Option<String>,
    sessions: Vec<(SessionIndexEntry, Option<String>)>,
}

fn secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9])(?:(?:api[_-]?key|password|passwd|passcode|passphrase|otp|one[\s_-]?time[\s_-]?(?:password|passcode|code)|verification[\s_-]?code|security[\s_-]?code|recovery[\s_-]?code|mfa[\s_-]?code|2fa[\s_-]?code|credential|private[_-]?key|client[_-]?secret|access[_-]?key|(?:api|auth|access|refresh|bearer)[\s_-]?token|session[\s_-]*(?:cookie|token|id))[\"']?\s*(?::|=|\bis\b)|cookie[\"']?\s*(?::|=))\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("secret assignment regex must compile")
    })
}

fn generic_secret_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:(?:^|[\s(\"'])(?:secret|token|pin)[\"']?\s*(?::|=)|(?:^|[^a-z0-9])(?:my|our|your)\s+(?:secret|token|pin)[\"']?\s+\bis\b)\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("generic secret assignment regex must compile")
    })
}

fn environment_credential_assignment_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:^|[^a-z0-9_])(?P<name>(?:[a-z][a-z0-9]*(?:_[a-z0-9]+)*_(?:access_key_id|secret_key_base|api_key|access_key|secret_key|private_key|client_key|auth_key|signing_key|encryption_key|token|pat|secret|password|passcode|pin|otp|pass|pwd)|secret_key_base|pgpassword))\s*(?::|=)\s*[\"']?[^\s\"',;}]+"#,
        )
        .expect("environment credential assignment regex must compile")
    })
}

fn contains_environment_credential_assignment(value: &str) -> bool {
    environment_credential_assignment_pattern()
        .captures_iter(value)
        .any(|captures| {
            // `max_token` is a common model-budget setting rather than a
            // credential. Keep this exact-name compatibility exception narrow
            // but case-insensitive, matching the surrounding detector;
            // prefixed service/CI variables remain secret regardless of case.
            captures
                .name("name")
                .is_some_and(|name| !name.as_str().eq_ignore_ascii_case("max_token"))
        })
}

fn known_secret_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)(?:\bsk-(?:proj-)?[a-z0-9_-]{12,}|\bgh[pousr]_[a-z0-9]{20,}|\bgithub_pat_[a-z0-9_]{20,}|\bxox[baprs]-[a-z0-9-]{10,}|\bAIza[a-z0-9_-]{20,}|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|\beyJ[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}\.[a-z0-9_-]{8,})",
        )
        .expect("known secret regex must compile")
    })
}

fn authorization_secret_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)(?:\b(?:proxy-)?authorization\s*:\s*[^\r\n]+|\b(?:bearer|basic)\s+[a-z0-9._~+/=-]+)",
        )
            .expect("authorization secret regex must compile")
    })
}

fn credential_url_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]{0,128}:[^/\s@]{1,128}@")
            .expect("credential URL regex must compile")
    })
}

fn looks_like_technical_path_token(token: &str) -> bool {
    token.starts_with('/') || token.matches('/').count() >= 2
}

fn ascii_shannon_entropy(token: &str) -> f64 {
    let mut counts = [0usize; 128];
    for byte in token.bytes() {
        if byte.is_ascii() {
            counts[byte as usize] += 1;
        }
    }
    let length = token.len() as f64;
    counts
        .into_iter()
        .filter(|count| *count > 0)
        .map(|count| {
            let probability = count as f64 / length;
            -probability * probability.log2()
        })
        .sum()
}

fn contains_high_entropy_secret_token(value: &str) -> bool {
    value
        .split(|character: char| {
            !(character.is_ascii_alphanumeric()
                || matches!(character, '-' | '_' | '.' | '+' | '/' | '='))
        })
        .any(|token| {
            let length = token.len();
            if !(24..=4_096).contains(&length) || token.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return false;
            }
            if looks_like_technical_path_token(token) {
                return false;
            }
            let distinct_bytes = token.bytes().collect::<HashSet<_>>().len();
            distinct_bytes >= 12
                && ascii_shannon_entropy(token) >= 4.0
                && token.bytes().any(|byte| byte.is_ascii_lowercase())
                && token.bytes().any(|byte| byte.is_ascii_uppercase())
                && token.bytes().any(|byte| byte.is_ascii_digit())
        })
}

fn contains_secret_like_value(value: &str) -> bool {
    value.contains("-----BEGIN PRIVATE KEY-----")
        || value.contains("-----BEGIN RSA PRIVATE KEY-----")
        || value.contains("-----BEGIN EC PRIVATE KEY-----")
        || value.contains("-----BEGIN OPENSSH PRIVATE KEY-----")
        || secret_assignment_pattern().is_match(value)
        || generic_secret_assignment_pattern().is_match(value)
        || contains_environment_credential_assignment(value)
        || known_secret_pattern().is_match(value)
        || authorization_secret_pattern().is_match(value)
        || credential_url_pattern().is_match(value)
        || contains_high_entropy_secret_token(value)
}

fn sanitize_extraction_source(value: &str) -> String {
    if contains_secret_like_value(value) {
        REDACTED_EXTRACTION_SOURCE.to_string()
    } else {
        value.to_string()
    }
}

fn durable_candidate_is_secret_safe(candidate: &DurableExtractionCandidate) -> bool {
    !contains_secret_like_value(&candidate.title)
        && !contains_secret_like_value(&candidate.content)
        && !contains_secret_like_value(&format!("{}: {}", candidate.title, candidate.content))
        && candidate
            .tags
            .iter()
            .all(|tag| !contains_secret_like_value(tag))
}

fn ledger_candidate_is_secret_safe(candidate: &LedgerExtractionCandidate) -> bool {
    !contains_secret_like_value(&candidate.title)
        && candidate.excerpt.as_deref().is_none_or(|excerpt| {
            !contains_secret_like_value(excerpt)
                && !contains_secret_like_value(&format!("{}: {excerpt}", candidate.title))
        })
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
    pending_history_revision: Option<&str>,
) -> Vec<Option<String>> {
    if pending_history_revision.is_some() {
        return build_history_rewrite_extraction_batches(session, extraction_watermark)
            .into_iter()
            .map(Some)
            .collect();
    }
    let has_retrieval_boundary = session
        .compression_events
        .iter()
        .any(|event| event.kind == CompressionEventKind::RetrievalWindow);
    let mut retrieval_sources = if has_retrieval_boundary {
        build_retrieval_window_extraction_batches(
            session,
            extraction_watermark,
            retrieval_source_acknowledged,
        )
    } else {
        Vec::new()
    };
    if let Some(summary) = session.conversation_summary.as_ref() {
        let summary = sanitize_extraction_source(&summary.content);
        // A configured summary fallback is a useful additional source, but it
        // cannot replace the pending retrieval delta: summary compression only
        // sees active messages, so archived turns would otherwise be lost when
        // the shared extraction watermark advances.
        let mut sources = retrieval_sources.drain(..).map(Some).collect::<Vec<_>>();
        sources.push(Some(summary.clone()));
        sources.extend(
            build_message_revision_extraction_batches(
                session,
                extraction_watermark,
                Some(&summary),
            )
            .into_iter()
            .map(Some),
        );
        return sources;
    }
    if has_retrieval_boundary {
        return if retrieval_sources.is_empty() {
            vec![None]
        } else {
            retrieval_sources.into_iter().map(Some).collect()
        };
    }
    let outline =
        derive_session_outline(session).map(|outline| sanitize_extraction_source(&outline));
    let mut sources = vec![outline.clone()];
    sources.extend(
        build_message_revision_extraction_batches(
            session,
            extraction_watermark,
            outline.as_deref(),
        )
        .into_iter()
        .map(Some),
    );
    sources
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

#[derive(Debug, Clone, Copy)]
enum ExtractionDeltaKind {
    RetrievalWindow,
    MessageRevision,
    HistoryRewrite,
}

impl ExtractionDeltaKind {
    fn title(self) -> &'static str {
        match self {
            Self::RetrievalWindow => "Retrieval-window",
            Self::MessageRevision => "Message revision",
            Self::HistoryRewrite => "History rewrite",
        }
    }

    fn event_count_label(self) -> &'static str {
        match self {
            Self::RetrievalWindow => "eligible_retrieval_events",
            Self::MessageRevision => "eligible_revision_events",
            Self::HistoryRewrite => "current_transcript_messages",
        }
    }

    fn marker(self) -> &'static str {
        match self {
            Self::RetrievalWindow => "retrieval_delta",
            Self::MessageRevision => "message_revision_delta",
            Self::HistoryRewrite => "history_rewrite_delta",
        }
    }
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

fn render_extraction_source_batches(
    source_items: Vec<RetrievalExtractionSourceItem>,
    extraction_watermark: Option<DateTime<Utc>>,
    eligible_event_count: usize,
    eligible_message_count: usize,
    kind: ExtractionDeltaKind,
) -> Vec<String> {
    if source_items.is_empty() {
        return Vec::new();
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
            let mut rendered = format!("# {} extraction delta v1\n\n", kind.title());
            rendered.push_str(&format!(
                "- extraction_watermark: {}\n- batch: {}/{}\n- {}: {}\n- eligible_messages: {}\n- source_items_in_batch: {}\n- distinct_messages_in_batch: {}\n- continuation_overlap_items_in_batch: {}\n- max_source_items_per_batch: {}\n- max_characters_per_batch: {}\n- truncated: false\n- continuation: {}\n",
                extraction_watermark
                    .map(|watermark| watermark.to_rfc3339())
                    .unwrap_or_else(|| "(none)".to_string()),
                batch_index + 1,
                batch_count,
                kind.event_count_label(),
                eligible_event_count,
                eligible_message_count,
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
            rendered.push_str(&format!(
                "\n[{}_{}]\n",
                kind.marker(),
                if batch_index + 1 < batch_count {
                    "continues_in_next_batch"
                } else {
                    "final_batch"
                }
            ));
            debug_assert!(rendered.chars().count() <= RETRIEVAL_EXTRACTION_MAX_CHARS);
            rendered
        })
        .collect()
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

/// Session-note bodies are already supplied from Jiandu Session topics. Tool
/// results may echo arbitrary tool output, including credentials, so retrieval
/// extraction retains only non-content acknowledgement metadata from a proven
/// `session_note` call. All other tool results are excluded.
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

fn build_message_revision_extraction_batches(
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
    covered_source: Option<&str>,
) -> Vec<String> {
    let history_artifact_ids = session_history_search_artifact_ids(session);
    let session_note_call_ids = session_note_tool_call_ids(session);
    let eligible_messages = session
        .messages
        .iter()
        .enumerate()
        .filter_map(|(message_index, message)| {
            let content_updated_at = message.content_updated_at()?;
            if extraction_watermark.is_some_and(|watermark| content_updated_at <= watermark)
                || matches!(message.role, Role::System)
                || history_artifact_ids.contains(&message.id)
            {
                return None;
            }
            let (role, content) = match message.role {
                Role::User => ("user", sanitize_extraction_source(&message.content)),
                Role::Assistant => ("assistant", sanitize_extraction_source(&message.content)),
                Role::Tool => (
                    "tool",
                    sanitized_session_note_result(message, &session_note_call_ids)?,
                ),
                Role::System => return None,
            };
            let content = content.trim();
            if content.is_empty()
                || (content.chars().count() <= 300
                    && covered_source.is_some_and(|source| source.contains(content)))
            {
                return None;
            }
            Some((message_index, role, content.to_string()))
        })
        .collect::<Vec<_>>();
    if eligible_messages.is_empty() {
        return Vec::new();
    }

    let mut source_items = Vec::new();
    for (message_index, role, extraction_content) in &eligible_messages {
        let segments = split_retrieval_extraction_content(extraction_content);
        let segment_count = segments.len();
        for (segment_index, content) in segments.into_iter().enumerate() {
            source_items.push(RetrievalExtractionSourceItem {
                source_item_ordinal: source_items.len() + 1,
                session_message_ordinal: *message_index + 1,
                role,
                retrieval_event_ordinal: None,
                content_segment_ordinal: segment_index + 1,
                content_segment_count: segment_count,
                content,
            });
        }
    }

    render_extraction_source_batches(
        source_items,
        extraction_watermark,
        eligible_messages.len(),
        eligible_messages.len(),
        ExtractionDeltaKind::MessageRevision,
    )
}

/// Rebuild the complete current transcript after an explicit history rewrite.
///
/// Memory derived from the previous transcript generation is superseded only
/// after these bounded batches have been checkpointed and persisted. System
/// context and generated history-search artifacts remain outside the durable
/// extraction source, matching the retrieval-window privacy boundary.
fn build_history_rewrite_extraction_batches(
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
) -> Vec<String> {
    let history_artifact_ids = session_history_search_artifact_ids(session);
    let session_note_call_ids = session_note_tool_call_ids(session);
    let eligible_messages = session
        .messages
        .iter()
        .enumerate()
        .filter_map(|(message_index, message)| {
            if matches!(message.role, Role::System) || history_artifact_ids.contains(&message.id) {
                return None;
            }
            let (role, content) = match message.role {
                Role::User => ("user", sanitize_extraction_source(&message.content)),
                Role::Assistant => ("assistant", sanitize_extraction_source(&message.content)),
                Role::Tool => (
                    "tool",
                    sanitized_session_note_result(message, &session_note_call_ids)?,
                ),
                Role::System => return None,
            };
            let content = content.trim();
            (!content.is_empty()).then_some((message_index, role, content.to_string()))
        })
        .collect::<Vec<_>>();

    let mut source_items = Vec::new();
    for (message_index, role, extraction_content) in &eligible_messages {
        let segments = split_retrieval_extraction_content(extraction_content);
        let segment_count = segments.len();
        for (segment_index, content) in segments.into_iter().enumerate() {
            source_items.push(RetrievalExtractionSourceItem {
                source_item_ordinal: source_items.len() + 1,
                session_message_ordinal: *message_index + 1,
                role,
                retrieval_event_ordinal: None,
                content_segment_ordinal: segment_index + 1,
                content_segment_count: segment_count,
                content,
            });
        }
    }

    let batches = render_extraction_source_batches(
        source_items,
        extraction_watermark,
        eligible_messages.len(),
        eligible_messages.len(),
        ExtractionDeltaKind::HistoryRewrite,
    );
    if batches.is_empty() {
        vec![format!(
            "# History rewrite extraction delta v1\n\n- extraction_watermark: {}\n- eligible_messages: 0\n- truncated: false\n- continuation: final_batch\n\n[history_rewrite_delta_final_batch]\n",
            extraction_watermark
                .map(|watermark| watermark.to_rfc3339())
                .unwrap_or_else(|| "(none)".to_string())
        )]
    } else {
        batches
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
    let eligible_messages = session
        .messages
        .iter()
        .enumerate()
        .filter_map(|(message_index, message)| {
            if matches!(message.role, Role::System) || history_artifact_ids.contains(&message.id) {
                return None;
            }
            let source_updated_at = message
                .content_updated_at()
                .filter(|updated_at| *updated_at > message.created_at)
                .unwrap_or(message.created_at);
            let source_changed_after_watermark =
                extraction_watermark.is_none_or(|watermark| source_updated_at > watermark);
            // A watermark older than the first retrieval event came from the
            // bounded ordinary outline, which did not cover all old messages.
            // The first retrieval boundary must therefore include every old
            // non-system source, including messages retained in the active
            // window. Once a retrieval event itself predates the watermark,
            // message creation or an explicit content-revision timestamp is
            // authoritative; later archive metadata alone cannot make an old
            // active message eligible twice.
            let first_retrieval_transition = !retrieval_source_acknowledged;
            if !first_retrieval_transition && !source_changed_after_watermark {
                return None;
            }
            let (role, content) = match message.role {
                Role::User => ("user", sanitize_extraction_source(&message.content)),
                Role::Assistant => ("assistant", sanitize_extraction_source(&message.content)),
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

    render_extraction_source_batches(
        source_items,
        extraction_watermark,
        eligible_event_ids.len(),
        eligible_messages.len(),
        ExtractionDeltaKind::RetrievalWindow,
    )
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

async fn resolve_session_project_id(
    ctx: &AutoDreamContext,
    session_id: &str,
) -> Result<Option<bamboo_domain::ProjectId>, String> {
    ctx.storage
        .load_session(session_id)
        .await
        .map_err(|error| {
            format!("failed to load candidate Project Session '{session_id}': {error}")
        })
        .map(|session| {
            session.and_then(|session| {
                ProjectContextResolver::memory_read_identity_for_session(&session)
            })
        })
}

async fn collect_candidate_sessions_for_project(
    ctx: &AutoDreamContext,
    project_key: &str,
    since: DateTime<Utc>,
) -> Result<Vec<(SessionIndexEntry, Option<String>)>, String> {
    let mut out = Vec::new();
    for (entry, summary) in collect_candidate_sessions(ctx, since).await {
        let Some(project_id) = resolve_session_project_id(ctx, &entry.id).await? else {
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
    Ok(out)
}

async fn collect_candidate_session_contexts_from_sessions(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    sessions: Vec<(SessionIndexEntry, Option<String>)>,
) -> Result<Vec<CandidateSessionContext>, String> {
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
                return Err(format!(
                    "failed to load canonical AutoDream extraction Session '{}': {error}",
                    entry.id
                ));
            }
        };
        let is_retrieval_window = session
            .compression_events
            .iter()
            .any(|event| event.kind == CompressionEventKind::RetrievalWindow);
        let retrieval_source_key = is_retrieval_window.then(|| retrieval_source_key(&session.id));
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
        let history_revision = history_rewrite_revision(&session, extraction_watermark);
        let pending_history_revision = if let Some(revision) = history_revision.as_deref() {
            match history_rewrite_is_acknowledged(ctx, &session, extraction_watermark, revision)
                .await
            {
                Ok(true) => None,
                Ok(false) => Some(revision.to_string()),
                Err(error) => {
                    tracing::warn!(
                        target: DREAM_TRACING_TARGET,
                        event = "history_rewrite_state_read_failed",
                        session_id = %entry.id,
                        "Could not verify history-rewrite completion state; replaying the bounded current transcript: {error}"
                    );
                    Some(revision.to_string())
                }
            }
        } else {
            None
        };
        if extraction_watermark.is_some_and(|watermark| watermark >= entry.updated_at)
            && (!is_retrieval_window || retrieval_source_acknowledged)
            && pending_history_revision.is_none()
        {
            continue;
        }
        let summaries = session_extraction_sources(
            &session,
            extraction_watermark,
            retrieval_source_acknowledged,
            pending_history_revision.as_deref(),
        );
        let project_key = ProjectContextResolver::memory_read_identity_for_session(&session)
            .map(bamboo_domain::ProjectId::into_string);
        let topics = sanitized_session_topics(
            memory
                .read_session_topics_with_content(&entry.id)
                .await
                .unwrap_or_default(),
        );
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
        if is_retrieval_window || pending_history_revision.is_some() {
            // Each bounded retrieval source batch gets its own provider budget.
            // Session topics use a separate unit so they cannot consume the
            // eight-candidate allowance needed by a source batch.
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
                    retrieval_source_key: retrieval_source_key.clone(),
                    history_revision: pending_history_revision.clone(),
                    transaction_owner_session_id: None,
                    transaction_source_updated_at: None,
                });
            }
            if !topics.is_empty() {
                out.push(CandidateSessionContext {
                    session_id: entry.id.clone(),
                    project_key,
                    entry,
                    summary: None,
                    topics,
                    retrieval_source_key,
                    history_revision: pending_history_revision,
                    transaction_owner_session_id: None,
                    transaction_source_updated_at: None,
                });
            }
        } else {
            for (source_index, summary) in summaries.into_iter().enumerate() {
                out.push(CandidateSessionContext {
                    session_id: entry.id.clone(),
                    project_key: project_key.clone(),
                    entry: entry.clone(),
                    summary,
                    topics: if source_index == 0 {
                        topics.clone()
                    } else {
                        Vec::new()
                    },
                    retrieval_source_key: None,
                    history_revision: None,
                    transaction_owner_session_id: None,
                    transaction_source_updated_at: None,
                });
            }
        }
    }
    Ok(out)
}

fn sanitized_session_topics(topics: Vec<(String, String)>) -> Vec<(String, String)> {
    topics
        .into_iter()
        .take(EXTRACTION_MAX_TOPICS_PER_SESSION)
        .map(|(topic, content)| {
            (
                sanitize_extraction_source(&topic),
                truncate_chars(
                    &sanitize_extraction_source(&content),
                    EXTRACTION_MAX_TOPIC_CHARS,
                ),
            )
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
    .expect("test candidate Sessions should load")
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtractionCheckpoint {
    version: u32,
    batch_id: String,
    session_key: String,
    source_updated_at: String,
    transaction_id: String,
    batch_index: usize,
    batch_count: usize,
    /// Fingerprint of the complete sanitized Session-topic snapshot that
    /// accompanied this transaction. Older v2 checkpoints omit this field;
    /// replay then conservatively resubmits the current topics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    topics_fingerprint: Option<String>,
    /// Explicit transcript generation whose prior Auto-Dream memories must be
    /// superseded after this complete transaction reaches both sinks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_revision: Option<String>,
    extracted: ExtractedCandidateBatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetrievalSourceState {
    version: u32,
    session_key: String,
    retrieval_source_key: String,
    source_updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryRewriteState {
    version: u32,
    session_key: String,
    history_revision: String,
    source_updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct MemoryReplacementTarget {
    id: String,
    scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    project_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct LedgerReplacementTarget {
    id: String,
    scope: LedgerScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    project_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryRewritePlan {
    version: u32,
    session_key: String,
    history_revision: String,
    source_updated_at: String,
    replacement_targets: Vec<MemoryReplacementTarget>,
    /// Exact non-Session lineage branches that must become active again before
    /// a mixed gardener descendant can be superseded safely.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    memory_reactivation_targets: Vec<MemoryReplacementTarget>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    preservation_session_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ledger_replacement_targets: Vec<LedgerReplacementTarget>,
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
    history_revision: Option<String>,
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
struct RebuiltSessionContexts {
    contexts: Vec<CandidateSessionContext>,
    topics_fingerprint: String,
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

fn extraction_transaction_session_id(session: &CandidateSessionContext) -> &str {
    session
        .transaction_owner_session_id
        .as_deref()
        .unwrap_or(&session.session_id)
}

fn extraction_transaction_source_updated_at(session: &CandidateSessionContext) -> String {
    session
        .transaction_source_updated_at
        .clone()
        .unwrap_or_else(|| session.entry.updated_at.to_rfc3339())
}

fn build_pending_extraction_batches(
    model: &str,
    sessions: &[CandidateSessionContext],
) -> Vec<PendingExtractionBatch> {
    sessions
        .iter()
        .enumerate()
        .map(|(context_index, session)| {
            let prompt = extraction_prompt(session);
            let transaction_session_id = extraction_transaction_session_id(session);
            let source_updated_at = extraction_transaction_source_updated_at(session);
            PendingExtractionBatch {
                context_index,
                checkpoint_id: extraction_checkpoint_id(
                    model,
                    transaction_session_id,
                    &source_updated_at,
                    &prompt,
                ),
                prompt,
                transaction_id: String::new(),
                batch_index: 0,
                batch_count: 0,
                topics_fingerprint: String::new(),
                history_revision: session.history_revision.clone(),
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
        let session_id = extraction_transaction_session_id(&sessions[pending.context_index]);
        indexes_by_session
            .entry(session_id.to_string())
            .or_default()
            .push(pending_index);
    }
    for (session_id, pending_indexes) in &indexes_by_session {
        let topics = pending_indexes
            .iter()
            .flat_map(|index| {
                sessions[pending_batches[*index].context_index]
                    .topics
                    .iter()
                    .cloned()
            })
            .collect::<Vec<_>>();
        let topics_fingerprint = extraction_topics_fingerprint(&topics);
        let checkpoint_ids = pending_indexes
            .iter()
            .map(|index| pending_batches[*index].checkpoint_id.clone())
            .collect::<Vec<_>>();
        let source_updated_at = extraction_transaction_source_updated_at(
            &sessions[pending_batches[pending_indexes[0]].context_index],
        );
        let transaction_id =
            extraction_transaction_id(session_id, &source_updated_at, &checkpoint_ids);
        let batch_count = pending_indexes.len();
        for (batch_index, pending_index) in pending_indexes.iter().enumerate() {
            let pending = &mut pending_batches[*pending_index];
            pending.transaction_id.clone_from(&transaction_id);
            pending.batch_index = batch_index;
            pending.batch_count = batch_count;
            pending.topics_fingerprint.clone_from(&topics_fingerprint);
        }
    }
    indexes_by_session
}

fn extraction_prompt(session: &CandidateSessionContext) -> String {
    let mut prompt = build_extraction_prompt(&[DreamCandidateInfo {
        session_id: session.session_id.clone(),
        title: sanitize_extraction_source(&session.entry.title),
        project_key: session.project_key.clone(),
        updated_at: session.entry.updated_at.to_rfc3339(),
        summary: session.summary.clone(),
        topics: session.topics.clone(),
    }]);
    if session.transaction_owner_session_id.is_some() {
        prompt.push_str(
            "\n\nThis Session is an unaffected source of a mixed-lineage durable-memory record. Re-extract its current durable facts so the canonical record can be replaced safely. Return ledger_candidates as an empty array; this preservation batch must not create prospective work.\n",
        );
    }
    prompt
}

fn extraction_checkpoint_id(
    model: &str,
    session_id: &str,
    source_updated_at: &str,
    prompt: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-extraction-checkpoint-v2\0");
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
    digest.update(b"bamboo-auto-dream-extraction-transaction-v2\0");
    digest.update(extraction_checkpoint_session_key(session_id).as_bytes());
    digest.update(b"\0");
    digest.update(source_updated_at.as_bytes());
    for checkpoint_id in checkpoint_ids {
        digest.update(b"\0");
        digest.update(checkpoint_id.as_bytes());
    }
    hex::encode(digest.finalize())
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
        && checkpoint.history_revision == pending.history_revision
}

fn extraction_checkpoint_session_key(session_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-extraction-session-v1\0");
    digest.update(session_id.as_bytes());
    hex::encode(digest.finalize())
}

fn retrieval_source_key(session_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-retrieval-source-v2\0");
    digest.update(session_id.as_bytes());
    hex::encode(digest.finalize())
}

/// Identify the current explicitly rewritten transcript without adding a
/// second Session cursor. The existing model-context boundary covers delete,
/// truncate, and restore operations; message revision metadata makes repeated
/// PATCH edits distinct even while those repairs coalesce into one pending
/// model-context epoch.
fn history_rewrite_revision(
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
) -> Option<String> {
    let authoritative_history_revision = session
        .model_context_state
        .as_ref()
        .map(|state| state.history_rewrite_revision)
        .filter(|revision| *revision > 0);
    let revised_messages = session
        .messages
        .iter()
        .filter_map(|message| {
            let updated_at = message.content_updated_at()?;
            Some((message, updated_at))
        })
        .collect::<Vec<_>>();
    let has_unacknowledged_revision = revised_messages.iter().any(|(_, updated_at)| {
        extraction_watermark.is_none_or(|watermark| *updated_at > watermark)
    });
    if authoritative_history_revision.is_none() && !has_unacknowledged_revision {
        return None;
    }

    let mut digest = Sha256::new();
    digest.update(b"bamboo-auto-dream-history-rewrite-v1\0");
    digest.update(session.id.as_bytes());
    if let Some(revision) = authoritative_history_revision {
        digest.update(b"\0authoritative-history\0");
        digest.update(revision.to_be_bytes());
    }
    for (message, updated_at) in revised_messages {
        digest.update(b"\0message\0");
        digest.update(message.id.as_bytes());
        digest.update(b"\0");
        digest.update(updated_at.to_rfc3339().as_bytes());
        digest.update(b"\0");
        digest.update(Sha256::digest(message.content.as_bytes()));
    }
    Some(hex::encode(digest.finalize()))
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
    retrieval_source_key: &str,
) -> PathBuf {
    extraction_checkpoint_session_dir(ctx, session_id).join(format!(
        "retrieval-source-v{RETRIEVAL_SOURCE_STATE_VERSION}-{retrieval_source_key}.json"
    ))
}

async fn retrieval_source_is_acknowledged(
    ctx: &AutoDreamContext,
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
) -> Result<bool, String> {
    if !session
        .compression_events
        .iter()
        .any(|event| event.kind == CompressionEventKind::RetrievalWindow)
    {
        return Ok(false);
    }
    let event_key = retrieval_source_key(&session.id);
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
        || state.retrieval_source_key != event_key
        || source_updated_at > session.updated_at
    {
        return Err("AutoDream retrieval source state identity mismatch".to_string());
    }
    Ok(extraction_watermark.is_some_and(|watermark| watermark >= source_updated_at))
}

async fn write_retrieval_source_state(
    ctx: &AutoDreamContext,
    session_id: &str,
    retrieval_source_key: &str,
    source_updated_at: &str,
) -> Result<(), String> {
    let state = RetrievalSourceState {
        version: RETRIEVAL_SOURCE_STATE_VERSION,
        session_key: extraction_checkpoint_session_key(session_id),
        retrieval_source_key: retrieval_source_key.to_string(),
        source_updated_at: source_updated_at.to_string(),
    };
    DateTime::parse_from_rfc3339(source_updated_at)
        .map_err(|error| format!("invalid retrieval source watermark: {error}"))?;
    let bytes = serde_json::to_vec_pretty(&state)
        .map_err(|error| format!("failed to serialize retrieval source state: {error}"))?;
    let path = retrieval_source_state_path(ctx, session_id, retrieval_source_key);
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
                || existing.retrieval_source_key != state.retrieval_source_key
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

fn history_rewrite_state_path(
    ctx: &AutoDreamContext,
    session_id: &str,
    history_revision: &str,
) -> PathBuf {
    extraction_checkpoint_session_dir(ctx, session_id).join(format!(
        "history-rewrite-state-v{HISTORY_REWRITE_STATE_VERSION}-{history_revision}.json"
    ))
}

fn history_rewrite_plan_path(
    ctx: &AutoDreamContext,
    session_id: &str,
    history_revision: &str,
) -> PathBuf {
    extraction_checkpoint_session_dir(ctx, session_id).join(format!(
        "history-rewrite-plan-v{HISTORY_REWRITE_PLAN_VERSION}-{history_revision}.json"
    ))
}

fn validate_history_revision(history_revision: &str) -> Result<(), String> {
    if history_revision.len() == 64
        && history_revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        Ok(())
    } else {
        Err("invalid AutoDream history-rewrite revision".to_string())
    }
}

async fn write_json_create_once(
    path: &Path,
    bytes: &[u8],
    temporary_prefix: &str,
) -> Result<bool, String> {
    let parent = path
        .parent()
        .ok_or_else(|| "AutoDream state path has no parent directory".to_string())?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| format!("failed to create AutoDream state directory: {error}"))?;
    let temporary_path = parent.join(format!(".{temporary_prefix}.{}.tmp", uuid::Uuid::new_v4()));
    let write_result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .await?;
        file.write_all(bytes).await?;
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
        Err(error) => Err(format!("failed to persist AutoDream state: {error}")),
    }
}

async fn history_rewrite_is_acknowledged(
    ctx: &AutoDreamContext,
    session: &Session,
    extraction_watermark: Option<DateTime<Utc>>,
    history_revision: &str,
) -> Result<bool, String> {
    validate_history_revision(history_revision)?;
    let path = history_rewrite_state_path(ctx, &session.id, history_revision);
    let raw = match tokio::fs::read(&path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(format!(
                "failed to read AutoDream history-rewrite state: {error}"
            ));
        }
    };
    let state = serde_json::from_slice::<HistoryRewriteState>(&raw)
        .map_err(|error| format!("failed to parse AutoDream history-rewrite state: {error}"))?;
    let source_updated_at = DateTime::parse_from_rfc3339(&state.source_updated_at)
        .map_err(|error| format!("invalid history-rewrite source timestamp: {error}"))?
        .with_timezone(&Utc);
    if state.version != HISTORY_REWRITE_STATE_VERSION
        || state.session_key != extraction_checkpoint_session_key(&session.id)
        || state.history_revision != history_revision
        || source_updated_at > session.updated_at
    {
        return Err("AutoDream history-rewrite state identity mismatch".to_string());
    }
    Ok(extraction_watermark.is_some_and(|watermark| watermark >= source_updated_at))
}

async fn write_history_rewrite_state(
    ctx: &AutoDreamContext,
    session_id: &str,
    history_revision: &str,
    source_updated_at: &str,
) -> Result<(), String> {
    validate_history_revision(history_revision)?;
    DateTime::parse_from_rfc3339(source_updated_at)
        .map_err(|error| format!("invalid history-rewrite source watermark: {error}"))?;
    let state = HistoryRewriteState {
        version: HISTORY_REWRITE_STATE_VERSION,
        session_key: extraction_checkpoint_session_key(session_id),
        history_revision: history_revision.to_string(),
        source_updated_at: source_updated_at.to_string(),
    };
    let bytes = serde_json::to_vec_pretty(&state)
        .map_err(|error| format!("failed to serialize history-rewrite state: {error}"))?;
    let path = history_rewrite_state_path(ctx, session_id, history_revision);
    if write_json_create_once(&path, &bytes, "history-rewrite-state").await? {
        return Ok(());
    }
    let existing = tokio::fs::read(&path)
        .await
        .map_err(|error| format!("failed to reread history-rewrite state: {error}"))?;
    let existing = serde_json::from_slice::<HistoryRewriteState>(&existing)
        .map_err(|error| format!("failed to parse existing history-rewrite state: {error}"))?;
    if existing.version != state.version
        || existing.session_key != state.session_key
        || existing.history_revision != state.history_revision
        || existing.source_updated_at != state.source_updated_at
    {
        return Err("existing AutoDream history-rewrite state mismatch".to_string());
    }
    Ok(())
}

async fn read_history_rewrite_plan(
    ctx: &AutoDreamContext,
    session_id: &str,
    history_revision: &str,
    current_source_updated_at: &str,
) -> Result<Option<HistoryRewritePlan>, String> {
    validate_history_revision(history_revision)?;
    let path = history_rewrite_plan_path(ctx, session_id, history_revision);
    let raw = match tokio::fs::read(path).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to read AutoDream history-rewrite plan: {error}"
            ));
        }
    };
    let plan = serde_json::from_slice::<HistoryRewritePlan>(&raw)
        .map_err(|error| format!("failed to parse AutoDream history-rewrite plan: {error}"))?;
    let plan_source = DateTime::parse_from_rfc3339(&plan.source_updated_at)
        .map_err(|error| format!("invalid history-rewrite plan timestamp: {error}"))?;
    let current_source = DateTime::parse_from_rfc3339(current_source_updated_at)
        .map_err(|error| format!("invalid current history-rewrite timestamp: {error}"))?;
    if plan.version != HISTORY_REWRITE_PLAN_VERSION
        || plan.session_key != extraction_checkpoint_session_key(session_id)
        || plan.history_revision != history_revision
        || plan_source > current_source
        || plan.preservation_session_ids.len() > HISTORY_REWRITE_MAX_PRESERVATION_SESSIONS
        || plan
            .preservation_session_ids
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != plan.preservation_session_ids.len()
        || plan.preservation_session_ids.iter().any(|session_id| {
            session_id.trim().is_empty()
                || session_id != session_id.trim()
                || session_id.contains('/')
                || session_id.contains('\\')
                || session_id.contains("..")
                || extraction_checkpoint_session_key(session_id) == plan.session_key
        })
        || plan
            .replacement_targets
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != plan.replacement_targets.len()
        || plan.replacement_targets.iter().any(|target| {
            target.id.trim().is_empty()
                || target.scope == MemoryScope::Session
                || (target.scope == MemoryScope::Project
                    && target.project_key.as_deref().is_none_or(str::is_empty))
                || (target.scope != MemoryScope::Project && target.project_key.is_some())
        })
        || plan
            .memory_reactivation_targets
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != plan.memory_reactivation_targets.len()
        || plan.memory_reactivation_targets.iter().any(|target| {
            target.id.trim().is_empty()
                || target.scope == MemoryScope::Session
                || (target.scope == MemoryScope::Project
                    && target.project_key.as_deref().is_none_or(str::is_empty))
                || (target.scope != MemoryScope::Project && target.project_key.is_some())
                || plan.replacement_targets.contains(target)
        })
        || plan.ledger_replacement_targets.iter().any(|target| {
            target.id.trim().is_empty()
                || (target.scope == LedgerScope::Project
                    && target.project_key.as_deref().is_none_or(str::is_empty))
                || (target.scope != LedgerScope::Project && target.project_key.is_some())
        })
        || plan
            .ledger_replacement_targets
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != plan.ledger_replacement_targets.len()
    {
        return Err("AutoDream history-rewrite plan identity mismatch".to_string());
    }
    Ok(Some(plan))
}

async fn write_history_rewrite_plan(
    ctx: &AutoDreamContext,
    session_id: &str,
    plan: &HistoryRewritePlan,
) -> Result<bool, String> {
    validate_history_revision(&plan.history_revision)?;
    let bytes = serde_json::to_vec_pretty(plan)
        .map_err(|error| format!("failed to serialize history-rewrite plan: {error}"))?;
    let path = history_rewrite_plan_path(ctx, session_id, &plan.history_revision);
    write_json_create_once(&path, &bytes, "history-rewrite-plan").await
}

fn memory_actor_is(document: &DurableMemoryDocument, actors: &[&str]) -> bool {
    document
        .frontmatter
        .created_by
        .actor
        .as_deref()
        .is_some_and(|actor| actors.contains(&actor))
        || document
            .frontmatter
            .updated_by
            .actor
            .as_deref()
            .is_some_and(|actor| actors.contains(&actor))
}

fn collect_memory_lineage_session_sources<'a>(
    document: &'a DurableMemoryDocument,
    documents_by_id: &HashMap<&'a str, &'a DurableMemoryDocument>,
    visited: &mut HashSet<&'a str>,
) -> HashSet<String> {
    let mut sources = document
        .frontmatter
        .sources
        .iter()
        .filter(|source| source.kind == "session")
        .map(|source| source.id.clone())
        .collect::<HashSet<_>>();
    if !visited.insert(document.frontmatter.id.as_str()) {
        return sources;
    }
    for ancestor in document
        .frontmatter
        .relations
        .supersedes
        .iter()
        .filter_map(|id| documents_by_id.get(id.as_str()).copied())
    {
        sources.extend(collect_memory_lineage_session_sources(
            ancestor,
            documents_by_id,
            visited,
        ));
    }
    sources
}

fn collect_memory_lineage_preservation_targets<'a>(
    document: &'a DurableMemoryDocument,
    documents_by_id: &HashMap<&'a str, &'a DurableMemoryDocument>,
    rewritten_session_id: &str,
    visited: &mut HashSet<&'a str>,
    preservation_session_ids: &mut HashSet<String>,
    memory_reactivation_targets: &mut HashSet<MemoryReplacementTarget>,
) {
    if !visited.insert(document.frontmatter.id.as_str()) {
        return;
    }
    for ancestor in document
        .frontmatter
        .relations
        .supersedes
        .iter()
        .filter_map(|id| documents_by_id.get(id.as_str()).copied())
    {
        let lineage_session_ids =
            collect_memory_lineage_session_sources(ancestor, documents_by_id, &mut HashSet::new());
        preservation_session_ids.extend(
            lineage_session_ids
                .iter()
                .filter(|source_id| source_id.as_str() != rewritten_session_id)
                .cloned(),
        );
        if !lineage_session_ids.contains(rewritten_session_id) {
            // This ancestor branch is wholly unaffected by the rewritten
            // Session. Reactivate its exact already-sanitized document before
            // superseding the mixed descendant. Canonical Session replay may
            // additionally refresh it, but an empty or lossy model response
            // can no longer erase the still-valid branch.
            memory_reactivation_targets.insert(MemoryReplacementTarget {
                id: ancestor.frontmatter.id.clone(),
                scope: ancestor.frontmatter.scope,
                project_key: ancestor.frontmatter.project_key.clone(),
            });
            continue;
        }
        collect_memory_lineage_preservation_targets(
            ancestor,
            documents_by_id,
            rewritten_session_id,
            visited,
            preservation_session_ids,
            memory_reactivation_targets,
        );
    }
}

#[derive(Debug)]
struct HistoryRewriteMemoryTargets {
    replacement_targets: Vec<MemoryReplacementTarget>,
    memory_reactivation_targets: Vec<MemoryReplacementTarget>,
    preservation_session_ids: Vec<String>,
}

type ExtractionMemoryFingerprint = (DurableMemoryType, String, String, String);

fn extraction_candidate_fingerprint(
    candidate: &DurableExtractionCandidate,
) -> Option<ExtractionMemoryFingerprint> {
    let memory_type = parse_candidate_type(&candidate.kind)?;
    let title = candidate.title.trim();
    let content = candidate.content.trim();
    let session_id = candidate.session_id.as_deref()?.trim();
    if title.is_empty() || content.is_empty() || session_id.is_empty() {
        return None;
    }
    Some((
        memory_type,
        title.to_string(),
        content.to_string(),
        session_id.to_string(),
    ))
}

fn memory_document_extraction_fingerprint(
    document: &DurableMemoryDocument,
) -> Option<ExtractionMemoryFingerprint> {
    let session_id = document
        .frontmatter
        .sources
        .iter()
        .find(|source| source.kind == "session")?
        .id
        .trim();
    if session_id.is_empty() {
        return None;
    }
    Some((
        document.frontmatter.r#type,
        document.frontmatter.title.trim().to_string(),
        document.body.trim().to_string(),
        session_id.to_string(),
    ))
}

fn memory_lineage_contains_frozen_target<'a>(
    document: &'a DurableMemoryDocument,
    documents_by_id: &HashMap<&'a str, &'a DurableMemoryDocument>,
    frozen_targets: &HashSet<MemoryReplacementTarget>,
    visited: &mut HashSet<&'a str>,
) -> bool {
    if !visited.insert(document.frontmatter.id.as_str()) {
        return false;
    }
    if frozen_targets.contains(&MemoryReplacementTarget {
        id: document.frontmatter.id.clone(),
        scope: document.frontmatter.scope,
        project_key: document.frontmatter.project_key.clone(),
    }) {
        return true;
    }
    document
        .frontmatter
        .relations
        .supersedes
        .iter()
        .filter_map(|id| documents_by_id.get(id.as_str()).copied())
        .any(|ancestor| {
            memory_lineage_contains_frozen_target(
                ancestor,
                documents_by_id,
                frozen_targets,
                visited,
            )
        })
}

fn memory_lineage_contains_retry_replacement<'a>(
    document: &'a DurableMemoryDocument,
    documents_by_id: &HashMap<&'a str, &'a DurableMemoryDocument>,
    retry_replacements: &HashSet<ExtractionMemoryFingerprint>,
    visited: &mut HashSet<&'a str>,
) -> bool {
    if !visited.insert(document.frontmatter.id.as_str()) {
        return false;
    }
    if memory_document_extraction_fingerprint(document)
        .is_some_and(|fingerprint| retry_replacements.contains(&fingerprint))
    {
        return true;
    }
    document
        .frontmatter
        .relations
        .supersedes
        .iter()
        .filter_map(|id| documents_by_id.get(id.as_str()).copied())
        .any(|ancestor| {
            memory_lineage_contains_retry_replacement(
                ancestor,
                documents_by_id,
                retry_replacements,
                visited,
            )
        })
}

async fn collect_history_rewrite_replacement_targets(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    session: &CandidateSessionContext,
    project_resolver: Option<&ProjectContextResolver>,
    current_store_is_project_scoped: bool,
) -> Result<HistoryRewriteMemoryTargets, String> {
    let frozen_targets = HashSet::new();
    let retry_replacements = HashSet::new();
    collect_history_rewrite_replacement_targets_for_retry(
        ctx,
        memory,
        session,
        project_resolver,
        current_store_is_project_scoped,
        &frozen_targets,
        &retry_replacements,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn collect_history_rewrite_replacement_targets_for_retry(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    session: &CandidateSessionContext,
    project_resolver: Option<&ProjectContextResolver>,
    current_store_is_project_scoped: bool,
    frozen_targets: &HashSet<MemoryReplacementTarget>,
    retry_replacements: &HashSet<ExtractionMemoryFingerprint>,
) -> Result<HistoryRewriteMemoryTargets, String> {
    // AutoDream can emit a Global candidate even while the explicit Project
    // maintenance path owns the current extraction store. Always inspect the
    // Global lineage, then add the applicable Project lineage, so one shared
    // Session watermark cannot strand stale facts in the other scope.
    let mut scopes = vec![(ctx.memory.clone(), MemoryScope::Global, None)];
    if current_store_is_project_scoped {
        let project_key = session.project_key.as_deref().ok_or_else(|| {
            "project-scoped history rewrite is missing project identity".to_string()
        })?;
        scopes.push((
            memory.clone(),
            MemoryScope::Project,
            Some(project_key.to_string()),
        ));
    } else if project_resolver.is_some() {
        if let Some(project_key) = session.project_key.as_deref() {
            let project_id = bamboo_domain::ProjectId::parse(project_key.to_string())
                .map_err(|error| format!("invalid history-rewrite Project identity: {error}"))?;
            scopes.push((
                ctx.memory.for_project(&project_id),
                MemoryScope::Project,
                Some(project_key.to_string()),
            ));
        }
    }

    let mut targets = HashSet::new();
    let mut preservation_session_ids = HashSet::new();
    let mut memory_reactivation_targets = HashSet::new();
    for (store, scope, project_key) in scopes {
        let documents = store
            .list_memory_documents(scope, project_key.as_deref())
            .await
            .map_err(|error| {
                format!("failed to inspect AutoDream memories before history rewrite: {error}")
            })?;
        let documents_by_id = documents
            .iter()
            .map(|document| (document.frontmatter.id.as_str(), document))
            .collect::<HashMap<_, _>>();
        for document in &documents {
            let lineage_session_ids = collect_memory_lineage_session_sources(
                document,
                &documents_by_id,
                &mut HashSet::new(),
            );
            let is_direct_auto_dream = document.frontmatter.updated_by.actor.as_deref()
                == Some(AUTO_DREAM_MEMORY_ACTOR)
                && lineage_session_ids.contains(&session.session_id);
            let is_gardener_descendant = memory_actor_is(document, &GARDENER_MEMORY_ACTORS)
                && lineage_session_ids.contains(&session.session_id);
            let descends_from_frozen_target = memory_lineage_contains_frozen_target(
                document,
                &documents_by_id,
                frozen_targets,
                &mut HashSet::new(),
            );
            let descends_from_retry_replacement = memory_lineage_contains_retry_replacement(
                document,
                &documents_by_id,
                retry_replacements,
                &mut HashSet::new(),
            );
            if document.frontmatter.status == DurableMemoryStatus::Active
                && (is_direct_auto_dream || is_gardener_descendant)
                && (descends_from_frozen_target || !descends_from_retry_replacement)
            {
                targets.insert(MemoryReplacementTarget {
                    id: document.frontmatter.id.clone(),
                    scope: document.frontmatter.scope,
                    project_key: document.frontmatter.project_key.clone(),
                });
                if is_gardener_descendant {
                    preservation_session_ids.extend(lineage_session_ids.iter().filter_map(
                        |source_id| (source_id != &session.session_id).then_some(source_id.clone()),
                    ));
                    collect_memory_lineage_preservation_targets(
                        document,
                        &documents_by_id,
                        &session.session_id,
                        &mut HashSet::new(),
                        &mut preservation_session_ids,
                        &mut memory_reactivation_targets,
                    );
                }
            }
        }
    }
    let mut targets = targets.into_iter().collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.scope
            .as_str()
            .cmp(right.scope.as_str())
            .then_with(|| left.project_key.cmp(&right.project_key))
            .then_with(|| left.id.cmp(&right.id))
    });
    let mut preservation_session_ids = preservation_session_ids.into_iter().collect::<Vec<_>>();
    preservation_session_ids.sort();
    if preservation_session_ids.len() > HISTORY_REWRITE_MAX_PRESERVATION_SESSIONS {
        return Err(format!(
            "history rewrite requires {} preservation Sessions; maximum is {}",
            preservation_session_ids.len(),
            HISTORY_REWRITE_MAX_PRESERVATION_SESSIONS
        ));
    }
    let mut memory_reactivation_targets =
        memory_reactivation_targets.into_iter().collect::<Vec<_>>();
    memory_reactivation_targets.sort_by(|left, right| {
        left.scope
            .as_str()
            .cmp(right.scope.as_str())
            .then_with(|| left.project_key.cmp(&right.project_key))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(HistoryRewriteMemoryTargets {
        replacement_targets: targets,
        memory_reactivation_targets,
        preservation_session_ids,
    })
}

#[allow(clippy::too_many_arguments)]
async fn revalidate_history_rewrite_plan(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    session: &CandidateSessionContext,
    plan: &HistoryRewritePlan,
    project_resolver: Option<&ProjectContextResolver>,
    current_store_is_project_scoped: bool,
    retry_replacements: &HashSet<ExtractionMemoryFingerprint>,
) -> Result<HistoryRewritePlan, String> {
    // A failed attempt releases the maintenance fence without acknowledging
    // the Session. Gardeners may advance either the frozen old lineage or a
    // partially persisted replacement before the next attempt acquires it.
    // Follow descendants of frozen targets, but exclude branches rooted only
    // in exact checkpoint candidates so corrected retry output is never
    // reclassified as stale input.
    let frozen_targets = plan
        .replacement_targets
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let refreshed = collect_history_rewrite_replacement_targets_for_retry(
        ctx,
        memory,
        session,
        project_resolver,
        current_store_is_project_scoped,
        &frozen_targets,
        retry_replacements,
    )
    .await?;

    let mut revalidated = plan.clone();
    revalidated
        .replacement_targets
        .extend(refreshed.replacement_targets);
    revalidated.replacement_targets.sort_by(|left, right| {
        left.scope
            .as_str()
            .cmp(right.scope.as_str())
            .then_with(|| left.project_key.cmp(&right.project_key))
            .then_with(|| left.id.cmp(&right.id))
    });
    revalidated.replacement_targets.dedup();

    revalidated
        .memory_reactivation_targets
        .extend(refreshed.memory_reactivation_targets);
    let replacement_targets = revalidated
        .replacement_targets
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    revalidated
        .memory_reactivation_targets
        .retain(|target| !replacement_targets.contains(target));
    revalidated
        .memory_reactivation_targets
        .sort_by(|left, right| {
            left.scope
                .as_str()
                .cmp(right.scope.as_str())
                .then_with(|| left.project_key.cmp(&right.project_key))
                .then_with(|| left.id.cmp(&right.id))
        });
    revalidated.memory_reactivation_targets.dedup();

    revalidated
        .preservation_session_ids
        .extend(refreshed.preservation_session_ids);
    revalidated.preservation_session_ids.sort();
    revalidated.preservation_session_ids.dedup();
    if revalidated.preservation_session_ids.len() > HISTORY_REWRITE_MAX_PRESERVATION_SESSIONS {
        return Err(format!(
            "history rewrite requires {} preservation Sessions after retry revalidation; maximum is {}",
            revalidated.preservation_session_ids.len(),
            HISTORY_REWRITE_MAX_PRESERVATION_SESSIONS
        ));
    }
    Ok(revalidated)
}

#[allow(clippy::too_many_arguments)]
async fn load_or_create_history_rewrite_plan(
    ctx: &AutoDreamContext,
    memory: &MemoryStore,
    ledger: &LedgerStore,
    session: &CandidateSessionContext,
    history_revision: &str,
    source_updated_at: &str,
    project_resolver: Option<&ProjectContextResolver>,
    current_store_is_project_scoped: bool,
) -> Result<HistoryRewritePlan, String> {
    if let Some(plan) = read_history_rewrite_plan(
        ctx,
        &session.session_id,
        history_revision,
        source_updated_at,
    )
    .await?
    {
        let retry_replacements = HashSet::new();
        return revalidate_history_rewrite_plan(
            ctx,
            memory,
            session,
            &plan,
            project_resolver,
            current_store_is_project_scoped,
            &retry_replacements,
        )
        .await;
    }
    let memory_targets = collect_history_rewrite_replacement_targets(
        ctx,
        memory,
        session,
        project_resolver,
        current_store_is_project_scoped,
    )
    .await?;
    let mut ledger_replacement_targets = ledger
        .list_records(LedgerScope::Global, None, &RecordFilter::default())
        .await
        .map_err(|error| {
            format!("failed to inspect AutoDream Ledger records before history rewrite: {error}")
        })?
        .into_iter()
        .filter(|document| {
            document.record.source.created_by == RecordActor::Extractor
                && document.record.source.session_id.as_deref() == Some(&session.session_id)
                && document.record.tags.iter().any(|tag| tag == "suggested")
        })
        .map(|document| LedgerReplacementTarget {
            id: document.record.id,
            scope: document.record.scope,
            project_key: document.record.project_key,
        })
        .collect::<Vec<_>>();
    ledger_replacement_targets.sort_by(|left, right| {
        left.scope
            .as_str()
            .cmp(right.scope.as_str())
            .then_with(|| left.project_key.cmp(&right.project_key))
            .then_with(|| left.id.cmp(&right.id))
    });
    let plan = HistoryRewritePlan {
        version: HISTORY_REWRITE_PLAN_VERSION,
        session_key: extraction_checkpoint_session_key(&session.session_id),
        history_revision: history_revision.to_string(),
        source_updated_at: source_updated_at.to_string(),
        replacement_targets: memory_targets.replacement_targets,
        memory_reactivation_targets: memory_targets.memory_reactivation_targets,
        preservation_session_ids: memory_targets.preservation_session_ids,
        ledger_replacement_targets,
    };
    if write_history_rewrite_plan(ctx, &session.session_id, &plan).await? {
        return Ok(plan);
    }
    let plan = read_history_rewrite_plan(
        ctx,
        &session.session_id,
        history_revision,
        source_updated_at,
    )
    .await?
    .ok_or_else(|| "concurrent history-rewrite plan disappeared before reuse".to_string())?;
    let retry_replacements = HashSet::new();
    revalidate_history_rewrite_plan(
        ctx,
        memory,
        session,
        &plan,
        project_resolver,
        current_store_is_project_scoped,
        &retry_replacements,
    )
    .await
}

async fn build_history_rewrite_preservation_contexts(
    ctx: &AutoDreamContext,
    owner_session_id: &str,
    plan: &HistoryRewritePlan,
) -> Result<Vec<CandidateSessionContext>, String> {
    let mut contexts = Vec::new();
    for source_session_id in &plan.preservation_session_ids {
        if source_session_id == owner_session_id {
            continue;
        }
        let session = ctx
            .storage
            .load_session(source_session_id)
            .await
            .map_err(|error| {
                format!(
                    "failed to load mixed-lineage preservation Session '{source_session_id}': {error}"
                )
            })?
            .ok_or_else(|| {
                format!(
                    "mixed-lineage preservation Session '{source_session_id}' no longer exists"
                )
            })?;
        let entry = ctx
            .session_store
            .get_index_entry(source_session_id)
            .await
            .ok_or_else(|| {
                format!(
                    "mixed-lineage preservation Session '{source_session_id}' has no canonical index entry"
                )
            })?;
        let project_key = ProjectContextResolver::memory_read_identity_for_session(&session)
            .map(bamboo_domain::ProjectId::into_string);
        let transaction_owner_session_id = Some(owner_session_id.to_string());
        let transaction_source_updated_at = Some(plan.source_updated_at.clone());
        for summary in build_history_rewrite_extraction_batches(&session, None) {
            contexts.push(CandidateSessionContext {
                entry: entry.clone(),
                summary: Some(summary),
                session_id: source_session_id.clone(),
                project_key: project_key.clone(),
                topics: Vec::new(),
                retrieval_source_key: None,
                history_revision: Some(plan.history_revision.clone()),
                transaction_owner_session_id: transaction_owner_session_id.clone(),
                transaction_source_updated_at: transaction_source_updated_at.clone(),
            });
        }
        let topics = sanitized_session_topics(
            ctx.memory
                .read_session_topics_with_content(source_session_id)
                .await
                .map_err(|error| {
                    format!(
                        "failed to read mixed-lineage preservation topics for '{source_session_id}': {error}"
                    )
                })?,
        );
        if !topics.is_empty() {
            contexts.push(CandidateSessionContext {
                entry,
                summary: None,
                session_id: source_session_id.clone(),
                project_key,
                topics,
                retrieval_source_key: None,
                history_revision: Some(plan.history_revision.clone()),
                transaction_owner_session_id,
                transaction_source_updated_at,
            });
        }
    }
    Ok(contexts)
}

fn history_rewrite_target_store(
    ctx: &AutoDreamContext,
    target: &MemoryReplacementTarget,
) -> Result<MemoryStore, String> {
    match target.scope {
        MemoryScope::Global => Ok(ctx.memory.clone()),
        MemoryScope::Project => {
            let project_key = target.project_key.as_deref().ok_or_else(|| {
                "history-rewrite Project target is missing project identity".to_string()
            })?;
            let project_id = bamboo_domain::ProjectId::parse(project_key.to_string())
                .map_err(|error| format!("invalid replacement Project identity: {error}"))?;
            Ok(ctx.memory.for_project(&project_id))
        }
        MemoryScope::Session => {
            Err("history-rewrite plan unexpectedly contains Session memory".to_string())
        }
    }
}

async fn supersede_history_rewrite_targets(
    ctx: &AutoDreamContext,
    ledger: &LedgerStore,
    plan: &HistoryRewritePlan,
) -> Result<(), String> {
    for target in &plan.memory_reactivation_targets {
        let store = history_rewrite_target_store(ctx, target)?;
        let restored = store
            .archive_memory(
                &target.id,
                target.project_key.as_deref(),
                DurableMemoryStatus::Active,
                Some("reactivated while replacing a mixed-lineage gardener memory"),
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to reactivate preserved memory '{}' after history rewrite: {error}",
                    target.id
                )
            })?;
        if restored.is_none() {
            return Err(format!(
                "preserved memory '{}' disappeared before history rewrite completion",
                target.id
            ));
        }
    }
    for target in &plan.replacement_targets {
        let store = history_rewrite_target_store(ctx, target)?;
        store
            .archive_memory(
                &target.id,
                target.project_key.as_deref(),
                DurableMemoryStatus::Superseded,
                Some("superseded after canonical Session history rewrite"),
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to supersede AutoDream memory '{}' after history rewrite: {error}",
                    target.id
                )
            })?;
    }
    for target in &plan.ledger_replacement_targets {
        ledger
            .transition_record(
                target.scope,
                target.project_key.as_deref(),
                &target.id,
                RecordStatus::Cancelled,
                Some("cancelled after canonical Session history rewrite"),
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to cancel AutoDream Ledger record '{}' after history rewrite: {error}",
                    target.id
                )
            })?;
    }
    Ok(())
}

async fn remove_history_rewrite_plan(
    ctx: &AutoDreamContext,
    session_id: &str,
    history_revision: &str,
) {
    let path = history_rewrite_plan_path(ctx, session_id, history_revision);
    if let Err(error) = tokio::fs::remove_file(path).await {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                target: DREAM_TRACING_TARGET,
                event = "history_rewrite_plan_cleanup_failed",
                session_id,
                history_revision,
                "Could not remove an acknowledged history-rewrite plan: {error}"
            );
        }
    }
}

fn extraction_checkpoint_path(
    ctx: &AutoDreamContext,
    session_id: &str,
    checkpoint_id: &str,
) -> PathBuf {
    extraction_checkpoint_session_dir(ctx, session_id).join(format!("{checkpoint_id}.json"))
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
    Ok(Some(checkpoint))
}

async fn write_extraction_checkpoint(
    path: &Path,
    checkpoint: &ExtractionCheckpoint,
) -> Result<bool, String> {
    let bytes = serde_json::to_vec_pretty(&checkpoint)
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
) -> Result<(Vec<StoredExtractionTransaction>, HashSet<String>), String> {
    let directory = extraction_checkpoint_session_dir(ctx, session_id);
    let mut entries = match tokio::fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), HashSet::new()));
        }
        Err(error) => {
            return Err(format!(
                "failed to inspect AutoDream extraction checkpoints: {error}"
            ));
        }
    };
    let expected_session_key = extraction_checkpoint_session_key(session_id);
    let mut grouped: HashMap<String, Vec<ExtractionCheckpoint>> = HashMap::new();
    let mut checkpoint_ids = HashSet::new();
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
        checkpoint_ids.insert(checkpoint.batch_id.clone());
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
                || batch.history_revision != first.history_revision
        }) {
            return Err("AutoDream extraction transaction metadata mismatch".to_string());
        }
        let is_complete = batches.len() == first.batch_count
            && batches
                .iter()
                .enumerate()
                .all(|(index, batch)| batch.batch_index == index);
        if !is_complete {
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
    Ok((complete, checkpoint_ids))
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
        if checkpoint_id.len() != 64 || !checkpoint_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            continue;
        }
        if retained_checkpoint_ids.contains(checkpoint_id) {
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

async fn rebuild_session_contexts_after_checkpoint_replay(
    ctx: &AutoDreamContext,
    template: &CandidateSessionContext,
    acknowledged_watermark: DateTime<Utc>,
    acknowledged_topics_fingerprint: Option<&str>,
) -> Result<RebuiltSessionContexts, String> {
    let session = ctx
        .storage
        .load_session(&template.session_id)
        .await
        .map_err(|error| {
            format!(
                "failed to reload Session source after checkpoint replay for {}: {error}",
                template.session_id
            )
        })?
        .ok_or_else(|| {
            format!(
                "Session source disappeared after checkpoint replay for {}",
                template.session_id
            )
        })?;
    let is_retrieval_window = session
        .compression_events
        .iter()
        .any(|event| event.kind == CompressionEventKind::RetrievalWindow);
    let retrieval_source_key = is_retrieval_window.then(|| retrieval_source_key(&session.id));
    let retrieval_source_acknowledged = if is_retrieval_window {
        retrieval_source_is_acknowledged(ctx, &session, Some(acknowledged_watermark)).await?
    } else {
        false
    };
    let project_key = ProjectContextResolver::memory_read_identity_for_session(&session)
        .map(bamboo_domain::ProjectId::into_string);
    let topics = sanitized_session_topics(
        ctx.memory
            .read_session_topics_with_content(&template.session_id)
            .await
            .map_err(|error| {
                format!(
                    "failed to reload Session topics after checkpoint replay for {}: {error}",
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

    let pending_history_revision = if let Some(revision) =
        history_rewrite_revision(&session, Some(acknowledged_watermark))
    {
        if history_rewrite_is_acknowledged(ctx, &session, Some(acknowledged_watermark), &revision)
            .await?
        {
            None
        } else {
            Some(revision)
        }
    } else {
        None
    };

    // Rebuild the newer canonical message delta against the watermark that
    // replay just acknowledged; reusing a pre-replay transition batch would
    // submit old messages to the provider again and invite stochastic duplicates.
    let summaries = session_extraction_sources(
        &session,
        Some(acknowledged_watermark),
        retrieval_source_acknowledged,
        pending_history_revision.as_deref(),
    );
    let mut contexts = Vec::new();
    if is_retrieval_window || pending_history_revision.is_some() {
        for summary in summaries.into_iter().filter(|summary| {
            summary
                .as_deref()
                .is_some_and(|content| !content.trim().is_empty())
        }) {
            contexts.push(CandidateSessionContext {
                entry: entry.clone(),
                summary,
                session_id: template.session_id.clone(),
                project_key: project_key.clone(),
                topics: Vec::new(),
                retrieval_source_key: retrieval_source_key.clone(),
                history_revision: pending_history_revision.clone(),
                transaction_owner_session_id: None,
                transaction_source_updated_at: None,
            });
        }
    } else {
        for (source_index, summary) in summaries.into_iter().enumerate() {
            contexts.push(CandidateSessionContext {
                entry: entry.clone(),
                summary,
                session_id: template.session_id.clone(),
                project_key: project_key.clone(),
                topics: if source_index == 0 && topics_changed {
                    topics.clone()
                } else {
                    Vec::new()
                },
                retrieval_source_key: None,
                history_revision: None,
                transaction_owner_session_id: None,
                transaction_source_updated_at: None,
            });
        }
    }

    // Topic files do not expose per-topic timestamps. New checkpoints carry a
    // fingerprint of the complete sanitized snapshot, so unchanged topics stay
    // deduplicated while any post-checkpoint edit is submitted as its own unit.
    // Legacy checkpoints lack the fingerprint and therefore replay current
    // topics conservatively rather than advancing past a possibly unseen note.
    if (is_retrieval_window || pending_history_revision.is_some())
        && topics_changed
        && !topics.is_empty()
    {
        contexts.push(CandidateSessionContext {
            entry,
            summary: None,
            session_id: template.session_id.clone(),
            project_key,
            topics,
            retrieval_source_key,
            history_revision: pending_history_revision,
            transaction_owner_session_id: None,
            transaction_source_updated_at: None,
        });
    }
    contexts.retain(|context| {
        !context.topics.is_empty()
            || context
                .summary
                .as_deref()
                .is_some_and(|summary| !summary.trim().is_empty())
    });
    Ok(RebuiltSessionContexts {
        contexts,
        topics_fingerprint: current_topics_fingerprint,
    })
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

    // A history-rewrite plan freezes the old Auto-Dream/gardener lineage before
    // provider calls and applies it only after every replacement sink succeeds.
    // Hold the same cross-process fence as the blob/dedup gardeners across that
    // whole interval so they cannot create an unfenced descendant. Ordinary
    // extraction shares the fence too: a stored rewrite checkpoint may need to
    // replay even when the freshly collected context no longer exposes it.
    let _memory_maintenance_fence = acquire_memory_maintenance_fence(memory).await?;

    let mut extraction_sessions = sessions.to_vec();
    let mut seen_sessions = HashSet::new();
    let session_source_watermarks = extraction_sessions
        .iter()
        .filter(|session| {
            seen_sessions.insert(extraction_transaction_session_id(session).to_string())
        })
        .map(|session| {
            (
                extraction_transaction_session_id(session).to_string(),
                extraction_transaction_source_updated_at(session),
            )
        })
        .collect::<Vec<_>>();
    let mut source_updated_at_by_session = session_source_watermarks
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>();
    let mut total_writes = ExtractionWrites::default();
    let mut pending_batches = build_pending_extraction_batches(model, &extraction_sessions);
    let pending_indexes_by_session =
        assign_pending_extraction_transactions(&extraction_sessions, &mut pending_batches);

    // A complete transaction proves that every provider batch was parsed and
    // checkpointed before any sink began. Replay it to both sinks and commit
    // its original source watermark before considering a newer prompt for the
    // same Session. This keeps stochastic rephrasing out of partial retries.
    let mut replayed_sources_by_session = HashMap::new();
    for (session_id, pending_indexes) in &pending_indexes_by_session {
        let (transactions, _) = load_extraction_checkpoint_transactions(ctx, session_id).await?;
        if transactions.is_empty() {
            let retained = pending_indexes
                .iter()
                .map(|index| pending_batches[*index].checkpoint_id.clone())
                .collect::<HashSet<_>>();
            remove_superseded_extraction_checkpoints(ctx, session_id, &retained).await?;
            continue;
        }

        let context_index = pending_batches[pending_indexes[0]].context_index;
        let session_context = &extraction_sessions[context_index];
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
        let mut replayed_source = None;

        for transaction in transactions {
            let source_watermark = DateTime::parse_from_rfc3339(&transaction.source_updated_at)
                .expect("stored checkpoint timestamp was validated while loading")
                .with_timezone(&Utc);
            if source_watermark > session_context.entry.updated_at {
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
            let history_revision = transaction
                .batches
                .first()
                .and_then(|batch| batch.history_revision.clone());
            if acknowledged_watermark.is_none_or(|watermark| watermark < source_watermark) {
                let history_plan = if let Some(revision) = history_revision.as_deref() {
                    let retry_replacements = transaction
                        .batches
                        .iter()
                        .flat_map(|batch| batch.extracted.memory.iter())
                        .filter_map(extraction_candidate_fingerprint)
                        .collect::<HashSet<_>>();
                    let plan = read_history_rewrite_plan(
                            ctx,
                            session_id,
                            revision,
                            &transaction.source_updated_at,
                        )
                        .await?
                        .ok_or_else(|| {
                            format!(
                                "history-rewrite checkpoint for {session_id} has no frozen replacement plan"
                            )
                        })?;
                    Some(
                        revalidate_history_rewrite_plan(
                            ctx,
                            memory,
                            session_context,
                            &plan,
                            project_resolver,
                            current_store_is_project_scoped,
                            &retry_replacements,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                for checkpoint in transaction.batches {
                    let writes = persist_durable_candidate_batch_with_project_resolver(
                        ctx,
                        memory,
                        ledger,
                        std::slice::from_ref(session_context),
                        checkpoint.extracted,
                        project_resolver,
                        current_store_is_project_scoped,
                        history_plan.as_ref(),
                    )
                    .await?;
                    total_writes.memory = total_writes.memory.saturating_add(writes.memory);
                    total_writes.ledger = total_writes.ledger.saturating_add(writes.ledger);
                }
                if let (Some(revision), Some(plan)) =
                    (history_revision.as_deref(), history_plan.as_ref())
                {
                    supersede_history_rewrite_targets(ctx, ledger, plan).await?;
                    write_history_rewrite_state(
                        ctx,
                        session_id,
                        revision,
                        &transaction.source_updated_at,
                    )
                    .await?;
                }
                if let Some(event_key) = extraction_sessions
                    .iter()
                    .filter(|session| session.session_id.as_str() == session_id.as_str())
                    .find_map(|session| session.retrieval_source_key.as_deref())
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
                replayed_source = Some(ReplayedExtractionSource {
                    watermark: source_watermark,
                    topics_fingerprint: transaction_topics_fingerprint,
                });
            }
            for checkpoint_id in checkpoint_ids {
                remove_extraction_checkpoint(ctx, session_id, &checkpoint_id).await;
            }
            if let Some(revision) = history_revision.as_deref() {
                remove_history_rewrite_plan(ctx, session_id, revision).await;
            }
        }
        remove_superseded_extraction_checkpoints(ctx, session_id, &HashSet::new()).await?;
        if let Some(replayed_source) = replayed_source {
            replayed_sources_by_session.insert(session_id.clone(), replayed_source);
        }
    }

    let mut sessions_to_rebuild = replayed_sources_by_session
        .iter()
        .filter_map(|(session_id, replayed_source)| {
            extraction_sessions
                .iter()
                .find(|session| session.session_id == *session_id)
                .filter(|session| session.entry.updated_at > replayed_source.watermark)
                .map(|_| session_id.clone())
        })
        .collect::<Vec<_>>();
    sessions_to_rebuild.sort();
    if !sessions_to_rebuild.is_empty() {
        let rebuild_set = sessions_to_rebuild.iter().cloned().collect::<HashSet<_>>();
        let mut rebuilt_contexts = Vec::new();
        let mut rebuilt_topics_fingerprints = HashMap::new();
        for session_id in &sessions_to_rebuild {
            let template = extraction_sessions
                .iter()
                .find(|session| session.session_id == *session_id)
                .cloned()
                .ok_or_else(|| {
                    format!("missing Session context after checkpoint replay for {session_id}")
                })?;
            let replayed_source = replayed_sources_by_session
                .get(session_id)
                .expect("rebuild Session came from acknowledged watermark map");
            let rebuilt = rebuild_session_contexts_after_checkpoint_replay(
                ctx,
                &template,
                replayed_source.watermark,
                replayed_source.topics_fingerprint.as_deref(),
            )
            .await?;
            if let Some(context) = rebuilt.contexts.first() {
                source_updated_at_by_session
                    .insert(session_id.clone(), context.entry.updated_at.to_rfc3339());
            }
            rebuilt_topics_fingerprints.insert(session_id.clone(), rebuilt.topics_fingerprint);
            rebuilt_contexts.extend(rebuilt.contexts);
        }
        pending_batches.retain(|pending| {
            !rebuild_set.contains(&extraction_sessions[pending.context_index].session_id)
        });
        for context in rebuilt_contexts {
            let context_index = extraction_sessions.len();
            let prompt = extraction_prompt(&context);
            let source_updated_at = context.entry.updated_at.to_rfc3339();
            let checkpoint_id =
                extraction_checkpoint_id(model, &context.session_id, &source_updated_at, &prompt);
            let topics_fingerprint = rebuilt_topics_fingerprints
                .get(&context.session_id)
                .cloned()
                .unwrap_or_else(|| extraction_topics_fingerprint(&[]));
            extraction_sessions.push(context);
            pending_batches.push(PendingExtractionBatch {
                context_index,
                prompt,
                checkpoint_id,
                transaction_id: String::new(),
                batch_index: 0,
                batch_count: 0,
                topics_fingerprint,
                history_revision: extraction_sessions[context_index].history_revision.clone(),
            });
        }
        assign_pending_extraction_transactions(&extraction_sessions, &mut pending_batches);
    }

    pending_batches.retain(|pending| {
        let session = &extraction_sessions[pending.context_index];
        replayed_sources_by_session
            .get(&session.session_id)
            .is_none_or(|replayed_source| session.entry.updated_at > replayed_source.watermark)
    });
    if pending_batches.is_empty() {
        return Ok(total_writes);
    }

    // Freeze the set of old Auto-Dream memories before any provider call or
    // replacement write. A retry must never reclassify partially persisted new
    // facts as old targets.
    let mut history_plans: HashMap<(String, String), HistoryRewritePlan> = HashMap::new();
    for pending in &pending_batches {
        let Some(history_revision) = pending.history_revision.as_deref() else {
            continue;
        };
        let session = &extraction_sessions[pending.context_index];
        let transaction_session_id = extraction_transaction_session_id(session);
        let key = (
            transaction_session_id.to_string(),
            history_revision.to_string(),
        );
        if history_plans.contains_key(&key) {
            continue;
        }
        let source_updated_at = source_updated_at_by_session
            .get(transaction_session_id)
            .expect("every pending Session has a source watermark");
        let plan = load_or_create_history_rewrite_plan(
            ctx,
            memory,
            ledger,
            session,
            history_revision,
            source_updated_at,
            project_resolver,
            current_store_is_project_scoped,
        )
        .await?;
        history_plans.insert(key, plan);
    }

    // Mixed-lineage gardener records are one canonical document backed by
    // several Sessions. Before superseding such a document for one rewritten
    // Session, checkpoint fresh extraction batches for every unaffected source
    // under the rewritten Session's transaction. Replay can then restore both
    // sides without advancing any unaffected Session watermark.
    let mut history_plan_keys = history_plans.keys().cloned().collect::<Vec<_>>();
    history_plan_keys.sort();
    let mut preservation_contexts = Vec::new();
    for key in history_plan_keys {
        let (owner_session_id, _) = &key;
        let plan = history_plans
            .get(&key)
            .expect("sorted history-rewrite plan key must remain present");
        preservation_contexts.extend(
            build_history_rewrite_preservation_contexts(ctx, owner_session_id, plan).await?,
        );
    }
    for context in preservation_contexts {
        let context_index = extraction_sessions.len();
        let prompt = extraction_prompt(&context);
        let transaction_session_id = extraction_transaction_session_id(&context);
        let source_updated_at = extraction_transaction_source_updated_at(&context);
        let checkpoint_id =
            extraction_checkpoint_id(model, transaction_session_id, &source_updated_at, &prompt);
        let history_revision = context.history_revision.clone();
        extraction_sessions.push(context);
        pending_batches.push(PendingExtractionBatch {
            context_index,
            prompt,
            checkpoint_id,
            transaction_id: String::new(),
            batch_index: 0,
            batch_count: 0,
            topics_fingerprint: String::new(),
            history_revision,
        });
    }
    assign_pending_extraction_transactions(&extraction_sessions, &mut pending_batches);

    let mut prepared_batches = Vec::with_capacity(pending_batches.len());
    let mut checkpoint_ids_by_session: HashMap<String, Vec<String>> = HashMap::new();
    for pending in &pending_batches {
        checkpoint_ids_by_session
            .entry(
                extraction_transaction_session_id(&extraction_sessions[pending.context_index])
                    .to_string(),
            )
            .or_default()
            .push(pending.checkpoint_id.clone());
    }
    for pending in pending_batches {
        let session = &extraction_sessions[pending.context_index];
        let transaction_session_id = extraction_transaction_session_id(session);
        let source_updated_at = source_updated_at_by_session
            .get(transaction_session_id)
            .expect("every pending Session has a source watermark");
        let checkpoint_path =
            extraction_checkpoint_path(ctx, transaction_session_id, &pending.checkpoint_id);
        let extracted = match read_extraction_checkpoint(&checkpoint_path, &pending.checkpoint_id)
            .await?
        {
            Some(checkpoint) => {
                if !checkpoint_matches_pending_batch(
                    &checkpoint,
                    transaction_session_id,
                    source_updated_at,
                    &pending,
                ) {
                    return Err("AutoDream pending checkpoint metadata mismatch".to_string());
                }
                checkpoint.extracted
            }
            None => {
                let mut extracted =
                    extract_durable_candidate_batch(provider, model, pending.prompt.clone())
                        .await?;
                if session.transaction_owner_session_id.is_some() {
                    extracted.ledger.clear();
                }
                let checkpoint = ExtractionCheckpoint {
                    version: EXTRACTION_CHECKPOINT_VERSION,
                    batch_id: pending.checkpoint_id.clone(),
                    session_key: extraction_checkpoint_session_key(transaction_session_id),
                    source_updated_at: source_updated_at.clone(),
                    transaction_id: pending.transaction_id.clone(),
                    batch_index: pending.batch_index,
                    batch_count: pending.batch_count,
                    topics_fingerprint: Some(pending.topics_fingerprint.clone()),
                    history_revision: pending.history_revision.clone(),
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
                        transaction_session_id,
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

    // Finish, parse, sanitize, and durably checkpoint every provider call
    // before either sink starts. If a later sink write fails, the retry reuses
    // these exact candidates instead of asking the model to rephrase them.
    for batch in prepared_batches {
        let session = &extraction_sessions[batch.context_index];
        let transaction_session_id = extraction_transaction_session_id(session);
        let history_plan = session.history_revision.as_ref().and_then(|revision| {
            history_plans.get(&(transaction_session_id.to_string(), revision.clone()))
        });
        let writes = persist_durable_candidate_batch_with_project_resolver(
            ctx,
            memory,
            ledger,
            std::slice::from_ref(&extraction_sessions[batch.context_index]),
            batch.extracted,
            project_resolver,
            current_store_is_project_scoped,
            history_plan,
        )
        .await?;
        total_writes.memory = total_writes.memory.saturating_add(writes.memory);
        total_writes.ledger = total_writes.ledger.saturating_add(writes.ledger);
    }

    // A retrieval-window session can span several bounded source batches. Only
    // acknowledge the captured Session update after every provider call and
    // both durable sinks have succeeded, so an omitted or failed remainder is
    // still eligible on the next AutoDream pass.
    for (session_id, checkpoint_ids) in checkpoint_ids_by_session {
        let source_updated_at = source_updated_at_by_session
            .get(&session_id)
            .expect("every checkpointed Session has a source watermark");
        let history_revision = extraction_sessions
            .iter()
            .filter(|session| extraction_transaction_session_id(session) == session_id)
            .find_map(|session| session.history_revision.as_deref());
        if let Some(revision) = history_revision {
            let plan = history_plans
                .get(&(session_id.clone(), revision.to_string()))
                .ok_or_else(|| format!("missing frozen history-rewrite plan for {session_id}"))?;
            supersede_history_rewrite_targets(ctx, ledger, plan).await?;
            write_history_rewrite_state(ctx, &session_id, revision, source_updated_at).await?;
        }
        if let Some(event_key) = extraction_sessions
            .iter()
            .filter(|session| extraction_transaction_session_id(session) == session_id)
            .find_map(|session| session.retrieval_source_key.as_deref())
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
        if let Some(revision) = history_revision {
            remove_history_rewrite_plan(ctx, &session_id, revision).await;
        }
    }

    Ok(total_writes)
}

async fn extract_durable_candidate_batch(
    provider: &Arc<dyn LLMProvider>,
    model: &str,
    prompt: String,
) -> Result<ExtractedCandidateBatch, String> {
    let base_prompt = prompt;
    let mut request_prompt = base_prompt.clone();
    let mut memory = Vec::new();
    let mut ledger = Vec::new();
    let mut memory_fingerprints = HashSet::new();
    let mut ledger_fingerprints = HashSet::new();

    for page_index in 0..EXTRACTION_MAX_PAGES_PER_SOURCE_BATCH {
        let raw = collect_stream_text(provider.clone(), model, request_prompt).await?;
        let page_memory = parse_extraction_candidates(&raw)?;
        if page_memory.len() > EXTRACTION_MAX_CANDIDATES {
            return Err(format!(
                "AutoDream extraction page returned {} candidates; maximum is {}",
                page_memory.len(),
                EXTRACTION_MAX_CANDIDATES
            ));
        }
        // Tolerant by design: absent/malformed ledger array → empty vec.
        let page_ledger = parse_ledger_candidates(&raw);
        if page_ledger.len() > EXTRACTION_MAX_CANDIDATES {
            return Err(format!(
                "AutoDream extraction page returned {} Ledger candidates; maximum is {}",
                page_ledger.len(),
                EXTRACTION_MAX_CANDIDATES
            ));
        }
        let source_exhausted =
            extraction_page_source_exhausted(&raw, page_memory.len(), page_ledger.len())?;
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

fn extraction_page_source_exhausted(
    raw: &str,
    memory_candidate_count: usize,
    ledger_candidate_count: usize,
) -> Result<bool, String> {
    let value = serde_json::from_str::<serde_json::Value>(strip_json_fence(raw))
        .map_err(|error| format!("failed to parse extraction page status: {error}"))?;
    match value.get("source_exhausted") {
        Some(serde_json::Value::Bool(exhausted)) => Ok(*exhausted),
        Some(_) => Err("AutoDream extraction source_exhausted must be a boolean".to_string()),
        None
            if memory_candidate_count < EXTRACTION_MAX_CANDIDATES
                && ledger_candidate_count < EXTRACTION_MAX_CANDIDATES =>
        {
            Ok(true)
        }
        None => Err(
            "AutoDream extraction saturated a memory or Ledger candidate page without source_exhausted; source watermark was not acknowledged"
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
The preceding response declared source_exhausted=false. Re-examine the same source, skip every candidate in already_returned, and return the next page only. Do not acknowledge exhaustion until every remaining durable-memory and Ledger candidate has been emitted.\n\
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
    history_plan: Option<&HistoryRewritePlan>,
) -> Result<ExtractionWrites, String> {
    let ExtractedCandidateBatch {
        memory: candidates,
        ledger: ledger_candidates,
    } = extracted;
    if candidates.len() > EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH {
        return Err(format!(
            "AutoDream extraction checkpoint contains {} memory candidates; maximum is {}",
            candidates.len(),
            EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH
        ));
    }
    if ledger_candidates.len() > EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH {
        return Err(format!(
            "AutoDream extraction checkpoint contains {} Ledger candidates; maximum is {}",
            ledger_candidates.len(),
            EXTRACTION_MAX_CANDIDATES_PER_SOURCE_BATCH
        ));
    }

    let mut session_project_keys = HashMap::new();
    for session in sessions {
        session_project_keys.insert(session.session_id.clone(), session.project_key.clone());
    }
    if let Some(plan) = history_plan {
        for session_id in &plan.preservation_session_ids {
            if session_project_keys.contains_key(session_id) {
                continue;
            }
            let source_session = ctx
                .storage
                .load_session(session_id)
                .await
                .map_err(|error| {
                    format!(
                        "failed to reload mixed-lineage preservation Session '{session_id}': {error}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "mixed-lineage preservation Session '{session_id}' no longer exists"
                    )
                })?;
            let project_key =
                ProjectContextResolver::memory_read_identity_for_session(&source_session)
                    .map(bamboo_domain::ProjectId::into_string);
            session_project_keys.insert(session_id.clone(), project_key);
        }
    }
    let replacement_targets = history_plan
        .map(|plan| {
            plan.replacement_targets
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    let mut writes = 0usize;
    let mut existing_by_scope: HashMap<
        (MemoryScope, Option<String>),
        HashSet<ExtractionMemoryFingerprint>,
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
                session_id,
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
                .filter(|document| {
                    !replacement_targets.contains(&MemoryReplacementTarget {
                        id: document.frontmatter.id.clone(),
                        scope: document.frontmatter.scope,
                        project_key: document.frontmatter.project_key.clone(),
                    })
                })
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

    let ledger_replacement_targets = history_plan
        .map(|plan| {
            plan.ledger_replacement_targets
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let ledger_writes =
        persist_ledger_candidates(ledger, ledger_candidates, &ledger_replacement_targets).await?;

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
    replacement_targets: &HashSet<LedgerReplacementTarget>,
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
        .filter(|document| {
            !replacement_targets.contains(&LedgerReplacementTarget {
                id: document.record.id.clone(),
                scope: document.record.scope,
                project_key: document.record.project_key.clone(),
            })
        })
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
    match generation_mode {
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
            let raw_body = collect_stream_text(provider.clone(), model, prompt).await?;
            normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)
        }
        DreamGenerationMode::Incremental => {
            let prompt =
                build_consolidation_prompt(&to_consolidation_sessions(&source_window.sessions));
            let raw_body = collect_stream_text(provider.clone(), model, prompt).await?;
            normalize_dream_notebook_body(&raw_body, DREAM_MAX_SUMMARY_CHARS)
        }
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
            collect_candidate_sessions_for_project(ctx, project_key, since).await?
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
    let provider_ref_enabled = config_snapshot.features.provider_model_ref;
    let model_ref = if provider_ref_enabled {
        config_snapshot
            .defaults
            .as_ref()
            .and_then(|d| d.memory_background.as_ref())
            .or_else(|| {
                config_snapshot
                    .defaults
                    .as_ref()
                    .and_then(|d| d.fast.as_ref())
            })
    } else {
        None
    };
    let (bg_provider, model): (Arc<dyn LLMProvider>, String) = if let Some(ref mr) = model_ref {
        let router = ProviderModelRouter::new(ctx.provider_registry.clone());
        let routed = router.route(mr).map_err(|e| {
            format!(
                "[auto_dream] failed to route background model ref '{}': {}",
                mr, e
            )
        })?;
        tracing::debug!(
            target: DREAM_TRACING_TARGET,
            model_ref = %mr,
            "Resolved background model via ProviderModelRef"
        );
        (routed, mr.model.clone())
    } else {
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
        (ctx.provider.clone(), model)
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
        collect_candidate_session_contexts_from_sessions(ctx, memory, sessions.clone()).await?;
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    struct PathBlockingProvider {
        inner: SequenceProvider,
        blocker_path: PathBuf,
    }

    #[async_trait]
    impl LLMProvider for PathBlockingProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            max_output_tokens: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            let stream = self
                .inner
                .chat_stream(messages, tools, max_output_tokens, model)
                .await?;
            tokio::fs::create_dir_all(
                self.blocker_path
                    .parent()
                    .expect("test blocker path has a parent"),
            )
            .await
            .expect("create test blocker parent");
            tokio::fs::write(&self.blocker_path, b"blocks records directory")
                .await
                .expect("create test blocker");
            Ok(stream)
        }
    }

    #[derive(Clone)]
    struct ToggleLoadStorage {
        inner: Arc<dyn Storage>,
        fail_loads: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Storage for ToggleLoadStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.inner.save_session(session).await
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            if self.fail_loads.load(Ordering::SeqCst) {
                return Err(std::io::Error::other(format!(
                    "injected load failure for {session_id}"
                )));
            }
            self.inner.load_session(session_id).await
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            self.inner.delete_session(session_id).await
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

    #[test]
    fn extraction_page_status_fails_closed_when_a_full_page_is_ambiguous() {
        let legacy = r#"{"candidates":[],"ledger_candidates":[]}"#;
        assert!(extraction_page_source_exhausted(legacy, EXTRACTION_MAX_CANDIDATES, 0).is_err());
        assert!(
            extraction_page_source_exhausted(legacy, 0, EXTRACTION_MAX_CANDIDATES).is_err(),
            "a saturated legacy Ledger page must not advance the source watermark"
        );
        assert!(extraction_page_source_exhausted(
            legacy,
            EXTRACTION_MAX_CANDIDATES - 1,
            EXTRACTION_MAX_CANDIDATES - 1,
        )
        .expect("an unsaturated legacy response is complete"));
        let continuation = r#"{"candidates":[],"ledger_candidates":[],"source_exhausted":false}"#;
        assert!(!extraction_page_source_exhausted(continuation, 0, 0)
            .expect("explicit continuation status"));
    }

    #[tokio::test]
    async fn extraction_pagination_fails_immediately_when_a_page_makes_no_progress() {
        let candidate = serde_json::json!({
            "title": "Repeated fact",
            "type": "reference",
            "scope": "global",
            "content": "The same safe fact is repeated.",
            "tags": ["paging"],
            "session_id": "paging-session"
        });
        let repeated_page = serde_json::json!({
            "candidates": [candidate],
            "ledger_candidates": [],
            "source_exhausted": false
        })
        .to_string();
        let sequence_provider = Arc::new(SequenceProvider::new(vec![
            repeated_page.clone(),
            repeated_page,
            serde_json::json!({
                "candidates": [],
                "ledger_candidates": [],
                "source_exhausted": true
            })
            .to_string(),
        ]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();

        let error = extract_durable_candidate_batch(&provider, "fast-model", "source".to_string())
            .await
            .expect_err("a repeated continuation page must fail closed");
        assert!(error.contains("made no safe, deduplicated progress"));
        assert_eq!(sequence_provider.recorded_prompts().len(), 2);
    }

    #[test]
    fn retrieval_delta_keeps_an_old_archived_identifier_beyond_the_recent_outline() {
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
        let sources = session_extraction_sources(&session, None, false, None);
        assert_eq!(sources.len(), 2);
        let delta = sources
            .iter()
            .filter_map(Option::as_deref)
            .collect::<String>();
        assert!(delta.contains("ARCHIVED_IDENTIFIER_ALPHA_947"));
        assert!(!delta.contains("SYSTEM_POLICY_MUST_NOT_APPEAR"));
    }

    #[test]
    fn retrieval_delta_applies_strict_event_and_message_watermarks() {
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
                "previously-active-now-archived",
                "PREVIOUSLY_EXTRACTED_ARCHIVED",
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
        let mut edited = message_at(
            Message::assistant("CORRECTED_OLD_ASSISTANT_FACT", None),
            "edited-old-assistant",
            test_time(4),
        );
        edited.mark_content_updated_at(test_time(23));
        session.messages.push(edited);

        let batches =
            build_retrieval_window_extraction_batches(&session, Some(test_time(20)), true);
        assert_eq!(batches.len(), 1);
        let delta = &batches[0];
        assert!(delta.contains("NEW_ARCHIVED"));
        assert!(delta.contains("NEW_ACTIVE"));
        assert!(delta.contains("CORRECTED_OLD_ASSISTANT_FACT"));
        assert!(!delta.contains("OLD_ARCHIVED"));
        assert!(!delta.contains("PREVIOUSLY_EXTRACTED_ARCHIVED"));
        assert!(!delta.contains("OLD_ACTIVE"));
        assert!(!delta.contains("EQUAL_ACTIVE"));
        assert!(delta.contains("eligible_retrieval_events: 1"));
        assert!(
            delta.find("NEW_ARCHIVED").expect("archived position")
                < delta.find("NEW_ACTIVE").expect("active position"),
            "messages must retain canonical Session order"
        );
    }

    #[test]
    fn explicit_message_revision_survives_derived_context_reset_and_watermarks_once() {
        let mut session = Session::new("edited-old-message", "model");
        let mut edited = message_at(
            Message::assistant("CORRECTED_DURABLE_FACT_AFTER_PATCH", None),
            "edited-old",
            test_time(2),
        );
        edited.mark_content_updated_at(test_time(40));
        session.messages.push(edited);
        for index in 0..8 {
            session.messages.push(message_at(
                Message::assistant(format!("recent message {index}"), None),
                &format!("recent-{index}"),
                test_time(20 + index),
            ));
        }

        let recent_outline = derive_session_outline(&session).expect("recent outline");
        assert!(!recent_outline.contains("CORRECTED_DURABLE_FACT_AFTER_PATCH"));
        let sources = session_extraction_sources(&session, Some(test_time(30)), false, None);
        assert_eq!(sources.len(), 2);
        assert!(sources
            .iter()
            .filter_map(Option::as_deref)
            .any(|source| source.contains("CORRECTED_DURABLE_FACT_AFTER_PATCH")));
        assert!(build_message_revision_extraction_batches(
            &session,
            Some(test_time(40)),
            Some(&recent_outline),
        )
        .is_empty());
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
        let delta = &batches[0];
        assert!(delta.contains("OLD_NEWLY_ARCHIVED"));
        assert!(delta.contains("OLD_RETAINED_ACTIVE"));
        assert!(delta.contains("NEW_ACTIVE"));
    }

    #[test]
    fn extraction_secret_filter_covers_sources_and_model_candidates() {
        for (case, value) in [
            ("tokenizer word", "tokenizer is tiktoken"),
            ("pinning word", "pinning is deterministic"),
            ("token suffix", "tokenization is lexical"),
            ("plain token concept", "token is a lexical unit"),
            ("token budget field", "max_token=1000"),
            ("uppercase token budget field", "MAX_TOKEN=1000"),
            ("mixed-case token budget field", "Max_Token=1000"),
            ("plain pin concept", "pin is a dependency reference"),
            ("ordinary suffix word", "monkey=abc"),
            ("project identifier", "PROJECT_KEY=abc"),
            ("primary identifier", "PRIMARY_KEY=id"),
            ("cache identifier", "CACHE_KEY=user-id"),
            ("working directory", "PWD=/workspace/project"),
            ("old working directory", "OLDPWD=/workspace/old"),
            ("ordinary bypass setting", "BYPASS=enabled"),
            ("ordinary compass setting", "COMPASS=north"),
            ("mixed-case user path", "/Users/Alice/Project2/config.toml"),
            ("mixed-case relative path", "src/HTTP2Client/Config.toml"),
            (
                "ordinary cookie preference",
                "My favorite cookie is chocolate",
            ),
        ] {
            assert!(
                !contains_secret_like_value(value),
                "ordinary technical case was classified as a secret: {case}"
            );
            assert!(
                sanitize_extraction_source(value) == value,
                "ordinary technical case was redacted: {case}"
            );
        }
        for (case, value) in [
            (
                "api key",
                "OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz",
            ),
            (
                "authorization",
                "Authorization: Bearer AbCdEfGhIjKlMnOpQrStUvWxYz123456",
            ),
            (
                "token authorization",
                "Authorization: Token 0123456789abcdef0123456789abcdef",
            ),
            (
                "hex session cookie",
                "session cookie: 0123456789abcdef0123456789abcdef",
            ),
            ("cookie assignment", "cookie=abc"),
            ("natural-language password", "my password is hunter2"),
            ("natural-language passcode", "my passcode is 1234"),
            ("possessive token", "my token is abc"),
            ("standalone token assignment", "TOKEN=abc"),
            ("prefixed token assignment", "GITHUB_TOKEN=abc"),
            ("personal access token assignment", "GITHUB_PAT=abc"),
            (
                "lowercase personal access token assignment",
                "gitlab_pat=abc",
            ),
            ("nested prefixed token assignment", "CI_JOB_TOKEN=abc"),
            ("lowercase prefixed token assignment", "github_token=abc"),
            ("secret key assignment", "STRIPE_SECRET_KEY=abc"),
            ("secret key base assignment", "SECRET_KEY_BASE=abc"),
            ("PostgreSQL password assignment", "PGPASSWORD=abc"),
            ("password alias assignment", "DB_PASS=abc"),
            ("password short alias assignment", "MYSQL_PWD=abc"),
            ("lowercase secret key assignment", "stripe_secret_key=abc"),
            ("key id assignment", "AWS_ACCESS_KEY_ID=abc"),
            (
                "temporary AWS access key assignment",
                "AWS_ACCESS_KEY_ID=ASIA1234567890ABCDEF",
            ),
            ("temporary AWS access key", "ASIA1234567890ABCDEF"),
            (
                "lowercase nested prefixed token assignment",
                "ci_job_token=abc",
            ),
            ("pin", "PIN: 1234"),
            ("three-digit pin", "PIN: 123"),
            (
                "credential URL",
                "postgres://user:password-value@example.test/database",
            ),
            (
                "password-only credential URL",
                "REDIS_URL=redis://:abc@example.test/0",
            ),
            (
                "standalone high-entropy token",
                "mF9Bx7Qa2cD8Zp4Ln6Rt3Vy5Kw1Hs0Je",
            ),
            (
                "high-entropy token with environment suffix",
                "mF9Bx7Qa2cD8Zp4Ln6Rt3Vy5Kw1Hs0Je.prod",
            ),
            ("private key", "-----BEGIN OPENSSH PRIVATE KEY-----"),
        ] {
            assert!(
                contains_secret_like_value(value),
                "secret case was not detected: {case}"
            );
            assert!(
                sanitize_extraction_source(value) == REDACTED_EXTRACTION_SOURCE,
                "secret case was not redacted: {case}"
            );
        }

        let unsafe_memory = DurableExtractionCandidate {
            title: "Provider credential".to_string(),
            kind: "reference".to_string(),
            content: "api_key=sk-proj-abcdefghijklmnopqrstuvwxyz".to_string(),
            scope: Some("global".to_string()),
            tags: vec!["provider".to_string()],
            session_id: Some("session-1".to_string()),
            confidence: Some("high".to_string()),
        };
        assert!(!durable_candidate_is_secret_safe(&unsafe_memory));

        let unsafe_hex_cookie_memory = DurableExtractionCandidate {
            title: "Session continuity".to_string(),
            kind: "reference".to_string(),
            content: "session cookie: 0123456789abcdef0123456789abcdef".to_string(),
            scope: Some("global".to_string()),
            tags: vec!["session".to_string()],
            session_id: Some("session-1".to_string()),
            confidence: Some("high".to_string()),
        };
        assert!(!durable_candidate_is_secret_safe(&unsafe_hex_cookie_memory));

        let unsafe_ledger = LedgerExtractionCandidate {
            title: "Rotate credential".to_string(),
            excerpt: Some("Authorization: Bearer AbCdEfGhIjKlMnOpQrStUvWxYz123456".to_string()),
            ..LedgerExtractionCandidate::default()
        };
        assert!(!ledger_candidate_is_secret_safe(&unsafe_ledger));

        let unsafe_hex_cookie_ledger = LedgerExtractionCandidate {
            title: "Session continuity".to_string(),
            excerpt: Some("session cookie: 0123456789abcdef0123456789abcdef".to_string()),
            ..LedgerExtractionCandidate::default()
        };
        assert!(!ledger_candidate_is_secret_safe(&unsafe_hex_cookie_ledger));

        for (title, content) in [
            ("Password", "hunter2"),
            ("Passcode", "1234"),
            ("PIN", "1234"),
            ("PIN", "123"),
            ("GITHUB_TOKEN", "abc"),
            ("github_token", "abc"),
            ("STRIPE_SECRET_KEY", "abc"),
            ("SECRET_KEY_BASE", "abc"),
            ("PGPASSWORD", "abc"),
            ("DB_PASS", "abc"),
            ("MYSQL_PWD", "abc"),
            ("AWS_ACCESS_KEY_ID", "ASIA1234567890ABCDEF"),
            ("Authorization", "Token 0123456789abcdef0123456789abcdef"),
        ] {
            let unsafe_memory = DurableExtractionCandidate {
                title: title.to_string(),
                kind: "reference".to_string(),
                content: content.to_string(),
                scope: Some("global".to_string()),
                tags: vec!["session".to_string()],
                session_id: Some("session-1".to_string()),
                confidence: Some("high".to_string()),
            };
            assert!(!durable_candidate_is_secret_safe(&unsafe_memory));
            let unsafe_ledger = LedgerExtractionCandidate {
                title: title.to_string(),
                excerpt: Some(content.to_string()),
                ..LedgerExtractionCandidate::default()
            };
            assert!(!ledger_candidate_is_secret_safe(&unsafe_ledger));
        }
    }

    #[tokio::test]
    async fn superseded_extraction_checkpoints_are_removed_per_session() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let memory = MemoryStore::new(temp_dir.path());
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: Arc::new(SequenceProvider::new(Vec::<String>::new())),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let extracted = ExtractedCandidateBatch {
            memory: Vec::new(),
            ledger: Vec::new(),
        };

        let source_updated_at = test_time(1).to_rfc3339();
        let stale_id = extraction_checkpoint_id("model", "session-a", &source_updated_at, "stale");
        let retained_id =
            extraction_checkpoint_id("model", "session-a", &source_updated_at, "retained");
        let other_id = extraction_checkpoint_id("model", "session-b", &source_updated_at, "other");
        let checkpoint = |session_id: &str, checkpoint_id: &str| ExtractionCheckpoint {
            version: EXTRACTION_CHECKPOINT_VERSION,
            batch_id: checkpoint_id.to_string(),
            session_key: extraction_checkpoint_session_key(session_id),
            source_updated_at: source_updated_at.clone(),
            transaction_id: extraction_transaction_id(
                session_id,
                &source_updated_at,
                &[checkpoint_id.to_string()],
            ),
            batch_index: 0,
            batch_count: 1,
            topics_fingerprint: Some(extraction_topics_fingerprint(&[])),
            history_revision: None,
            extracted: extracted.clone(),
        };
        let stale_path = extraction_checkpoint_path(&context, "session-a", &stale_id);
        let retained_path = extraction_checkpoint_path(&context, "session-a", &retained_id);
        let other_session_path = extraction_checkpoint_path(&context, "session-b", &other_id);
        assert!(
            write_extraction_checkpoint(&stale_path, &checkpoint("session-a", &stale_id))
                .await
                .expect("write stale checkpoint")
        );
        assert!(write_extraction_checkpoint(
            &retained_path,
            &checkpoint("session-a", &retained_id)
        )
        .await
        .expect("write retained checkpoint"));
        assert!(write_extraction_checkpoint(
            &other_session_path,
            &checkpoint("session-b", &other_id),
        )
        .await
        .expect("write other-session checkpoint"));

        remove_superseded_extraction_checkpoints(
            &context,
            "session-a",
            &HashSet::from([retained_id]),
        )
        .await
        .expect("remove superseded checkpoint");

        assert!(!stale_path.exists());
        assert!(retained_path.exists());
        assert!(other_session_path.exists());
    }

    #[test]
    fn retrieval_delta_excludes_self_history_system_and_non_content_payloads() {
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
            test_time(12),
        ));
        session.messages.push(message_at(
            Message::assistant(
                "Authorization: Bearer AbCdEfGhIjKlMnOpQrStUvWxYz123456",
                None,
            ),
            "credential-assistant",
            test_time(12),
        ));

        let call_id = "history-call";
        session.messages.push(message_at(
            Message::assistant(
                "SEARCH_QUERY_SECRET",
                Some(vec![ToolCall {
                    id: call_id.to_string(),
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
            test_time(13),
        ));
        session.messages.push(message_at(
            Message::tool_result(call_id, "SEARCH_RESULT_SECRET"),
            "history-result-message",
            test_time(14),
        ));
        let session_note_call_id = "session-note-call";
        session.messages.push(message_at(
            Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: session_note_call_id.to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "session_note".to_string(),
                        arguments: serde_json::json!({"action": "read"}).to_string(),
                    },
                }]),
            ),
            "session-note-call-message",
            test_time(15),
        ));
        session.messages.push(message_at(
            Message::tool_result(
                session_note_call_id,
                serde_json::json!({
                    "action": "read",
                    "session_id": "retrieval-delta-private-fields",
                    "topic": "continuity",
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
            test_time(16),
        ));
        session.messages.push(message_at(
            Message::tool_result("untrusted-tool-call", "TOOL_CREDENTIAL_SECRET"),
            "untrusted-tool-result",
            test_time(17),
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
            "AbCdEfGhIjKlMnOpQrStUvWxYz123456",
        ] {
            assert!(
                !delta.contains(excluded),
                "unexpected private field: {excluded}"
            );
        }
    }

    #[test]
    fn retrieval_delta_ordering_and_caps_are_deterministic() {
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
        assert!(batches[0].contains("continuation_overlap_items_in_batch: 0"));
        assert!(batches[0].contains("continuation: continues_in_next_batch"));
        assert!(batches[1].contains("continuation_overlap_content_suffix: \"ORDER_007\""));
        assert!(batches[8].contains("batch: 9/9"));
        assert!(batches[8].contains("source_items_in_batch: 2"));
        assert!(batches[8].contains("continuation_overlap_items_in_batch: 1"));
        assert!(batches[8].contains("continuation_overlap_content_suffix: \"ORDER_063\""));
        assert!(batches[8].contains("continuation: final_batch"));
        assert!(batches.iter().all(|batch| {
            batch.contains("eligible_messages: 66")
                && batch.contains("max_source_items_per_batch: 8")
                && batch.contains("truncated: false")
                && batch.chars().count() <= RETRIEVAL_EXTRACTION_MAX_CHARS
        }));
        let all_batches = batches.concat();
        for index in 0..66 {
            assert!(
                all_batches.contains(&format!("ORDER_{index:03}")),
                "message {index} must be retained"
            );
        }
        assert!(
            all_batches.find("ORDER_000").expect("first message")
                < all_batches.find("ORDER_065").expect("last message")
        );

        let mut oversized = Session::new("retrieval-delta-char-cap", "model");
        oversized
            .compression_events
            .push(retrieval_event("event", test_time(1)));
        let oversized_content = format!("CHAR_CAP_START{}", "x".repeat(20_000));
        oversized.messages.push(message_at(
            Message::user(oversized_content.clone()),
            "oversized",
            test_time(2),
        ));
        let capped_batches = build_retrieval_window_extraction_batches(&oversized, None, false);
        assert!(capped_batches.len() > 1);
        assert!(capped_batches.iter().all(|batch| {
            batch.contains("truncated: false")
                && batch.chars().count() <= RETRIEVAL_EXTRACTION_MAX_CHARS
        }));
        let reconstructed = capped_batches
            .iter()
            .flat_map(|batch| batch.lines())
            .filter_map(|line| line.strip_prefix("- content: "))
            .map(|json| serde_json::from_str::<String>(json).expect("JSON content segment"))
            .collect::<String>();
        assert_eq!(reconstructed, oversized_content);
    }

    #[test]
    fn retrieval_delta_survives_session_serialization_and_empty_delta_is_explicit() {
        let mut session = Session::new("retrieval-delta-restart", "model");
        session
            .compression_events
            .push(retrieval_event("event", test_time(10)));
        let mut archived = message_at(
            Message::user("RESTART_STABLE_CONTENT"),
            "archived",
            test_time(2),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event".to_string());
        session.messages.push(archived);

        let before = build_retrieval_window_extraction_batches(&session, None, false);
        let restored: Session = serde_json::from_slice(
            &serde_json::to_vec(&session).expect("serialize retrieval Session"),
        )
        .expect("restore retrieval Session");
        let after = build_retrieval_window_extraction_batches(&restored, None, false);
        assert_eq!(after, before);
        assert!(
            build_retrieval_window_extraction_batches(&restored, Some(test_time(10)), true)
                .is_empty()
        );
    }

    #[test]
    fn extraction_source_preserves_retrieval_delta_before_summary_fallback() {
        let mut summary_session = Session::new("retrieval-before-summary", "model");
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
        let sources = session_extraction_sources(&summary_session, None, false, None);
        assert_eq!(sources.len(), 2);
        assert!(sources[0]
            .as_deref()
            .expect("retrieval delta")
            .contains("retrieval content must not override summary"));
        assert_eq!(sources[1].as_deref(), Some("EXACT_SUMMARY_SOURCE"));

        let mut ordinary = Session::new("ordinary-outline", "model");
        ordinary
            .messages
            .push(Message::user("ORDINARY_RECENT_OUTLINE"));
        let sources = session_extraction_sources(&ordinary, None, false, None);
        assert_eq!(sources.len(), 1);
        let source = sources[0].as_deref().expect("outline");
        assert!(source.contains("ORDINARY_RECENT_OUTLINE"));
        assert!(!source.contains("Retrieval-window extraction delta"));
    }

    #[test]
    fn coalesced_history_rewrites_have_distinct_extraction_revisions() {
        let mut session = Session::new("coalesced-history-rewrites", "model");
        session.messages.push(Message::user("first retained fact"));
        session
            .messages
            .push(Message::assistant("first removed fact", None));
        session.messages.pop();
        session.clear_derived_context_state();
        let first = history_rewrite_revision(&session, None).expect("first rewrite revision");
        let first_state_revision = session
            .model_context_state
            .as_ref()
            .expect("model context state")
            .state_revision;

        session.messages.clear();
        session.clear_derived_context_state();
        let second = history_rewrite_revision(&session, None).expect("second rewrite revision");
        let second_state = session
            .model_context_state
            .as_ref()
            .expect("model context state");

        assert_eq!(
            second_state.prefix_epoch, 1,
            "pending epoch stays coalesced"
        );
        assert_eq!(second_state.state_revision, first_state_revision + 1);
        assert_eq!(second_state.history_rewrite_revision, 2);
        assert_ne!(
            second, first,
            "each mutation needs a distinct acknowledgement"
        );
    }

    #[test]
    fn internal_prompt_reset_does_not_create_a_history_rewrite() {
        let mut session = Session::new("internal-prompt-reset", "model");
        session
            .messages
            .push(Message::user("canonical transcript remains unchanged"));
        session.reset_model_context_epoch(
            bamboo_domain::ModelContextResetReason::ExplicitHistoryRewrite,
        );

        assert!(history_rewrite_revision(&session, None).is_none());
        assert_eq!(
            session
                .model_context_state
                .as_ref()
                .expect("model context state")
                .history_rewrite_revision,
            0
        );
    }

    #[tokio::test]
    async fn candidate_collection_uses_watermarked_retrieval_delta_and_keeps_topics_separate() {
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
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "FALLBACK_SUMMARY_ACTIVE_ONLY",
            1,
            10,
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
        let session_note_call_id = "session-note-call";
        session.messages.push(message_at(
            Message::assistant(
                "",
                Some(vec![ToolCall {
                    id: session_note_call_id.to_string(),
                    tool_type: "function".to_string(),
                    function: FunctionCall {
                        name: "session_note".to_string(),
                        arguments: serde_json::json!({"action": "append"}).to_string(),
                    },
                }]),
            ),
            "session-note-call",
            watermark + chrono::Duration::seconds(1),
        ));
        session.messages.push(message_at(
            Message::tool_result(
                session_note_call_id,
                serde_json::json!({
                    "action": "append",
                    "session_id": "retrieval-collection",
                    "topic": "continuity",
                    "path": "/must/not/reach/extraction",
                    "length_chars": 59,
                    "max_chars": 12000
                })
                .to_string(),
            ),
            "session-note-result",
            watermark + chrono::Duration::seconds(2),
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
                "SESSION_NOTE_AFTER_WATERMARK\nSESSION_TOPIC_REMAINS_SEPARATE",
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
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(Vec::<String>::new()));
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider,
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };

        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            now - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 3);
        let source = contexts[0].summary.as_deref().expect("retrieval source");
        assert!(source.contains("ARCHIVED_AFTER_WATERMARK_EVENT"));
        assert!(source.contains("session_note"));
        assert!(!source.contains("/must/not/reach/extraction"));
        assert!(source.contains("ACTIVE_BEFORE_WATERMARK"));
        assert!(contexts[0].topics.is_empty());
        assert_eq!(
            contexts[1].summary.as_deref(),
            Some("FALLBACK_SUMMARY_ACTIVE_ONLY")
        );
        assert!(contexts[1].topics.is_empty());
        assert!(contexts[2].summary.is_none());
        assert_eq!(
            contexts[2].topics,
            vec![
                (
                    "continuity".to_string(),
                    "SESSION_NOTE_AFTER_WATERMARK\nSESSION_TOPIC_REMAINS_SEPARATE".to_string()
                ),
                (
                    "provider-auth".to_string(),
                    REDACTED_EXTRACTION_SOURCE.to_string()
                )
            ]
        );

        let source_prompt = extraction_prompt(&contexts[0]);
        let topic_prompt = extraction_prompt(&contexts[2]);
        assert!(source_prompt.contains("ARCHIVED_AFTER_WATERMARK_EVENT"));
        assert!(!source_prompt.contains("SESSION_TOPIC_REMAINS_SEPARATE"));
        assert!(topic_prompt.contains("SESSION_NOTE_AFTER_WATERMARK"));
        assert!(topic_prompt.contains("SESSION_TOPIC_REMAINS_SEPARATE"));
        assert!(topic_prompt.contains("- session topics:"));
        assert!(!topic_prompt.contains("sk-proj-topicsecretabcdefghijkl"));
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
    async fn retrieval_source_identity_survives_derived_history_reset() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let watermark = Utc::now() - chrono::Duration::seconds(10);
        let mut session = Session::new("stable-retrieval-source", "model");
        session.compression_events.push(retrieval_event(
            "first-derived-event",
            watermark - chrono::Duration::seconds(2),
        ));
        let mut old_message = message_at(
            Message::user("OLD_SOURCE_MUST_NOT_REPLAY_FROM_EVENT_ID_CHURN"),
            "old-message",
            watermark - chrono::Duration::minutes(5),
        );
        old_message.compressed = true;
        old_message.compressed_by_event_id = Some("first-derived-event".to_string());
        session.messages.push(old_message);
        session.updated_at = watermark;
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .mark_session_extracted(&session.id, &watermark.to_rfc3339())
            .await
            .expect("seed extraction watermark");
        let context = AutoDreamContext {
            session_store,
            storage,
            memory,
            provider: Arc::new(SequenceProvider::new(Vec::<String>::new())),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let stable_key = retrieval_source_key(&session.id);
        write_retrieval_source_state(&context, &session.id, &stable_key, &watermark.to_rfc3339())
            .await
            .expect("write stable retrieval source state");

        session.clear_derived_context_state();
        session.compression_events.push(retrieval_event(
            "replacement-derived-event",
            watermark + chrono::Duration::seconds(1),
        ));
        session.messages[0].compressed = true;
        session.messages[0].compressed_by_event_id = Some("replacement-derived-event".to_string());
        session.updated_at = watermark + chrono::Duration::seconds(2);

        assert_eq!(retrieval_source_key(&session.id), stable_key);
        assert!(
            retrieval_source_is_acknowledged(&context, &session, Some(watermark))
                .await
                .expect("read stable retrieval state")
        );
        assert!(
            build_retrieval_window_extraction_batches(&session, Some(watermark), true).is_empty(),
            "a regenerated derived event must not turn old messages into a first-transition replay"
        );
    }

    #[tokio::test]
    async fn explicit_history_rewrite_supersedes_only_prior_auto_dream_memories() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let old_watermark = Utc::now() - chrono::Duration::minutes(1);
        let rewritten_at = old_watermark + chrono::Duration::seconds(10);
        let mut session = Session::new("history-rewrite-memory", "model");
        session.title = "History rewrite memory".to_string();
        session.messages.push(message_at(
            Message::assistant("The database is PostgreSQL.", None),
            "assistant-database",
            old_watermark - chrono::Duration::seconds(5),
        ));
        session.updated_at = old_watermark;
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        let old_auto = memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Canonical database",
                "The database is PostgreSQL.",
                &["database".to_string()],
                Some(&session.id),
                AUTO_DREAM_MEMORY_ACTOR,
                false,
                None,
            )
            .await
            .expect("seed old AutoDream memory");
        let gardener_split = memory
            .split_memory(
                &old_auto.frontmatter.id,
                None,
                &[
                    bamboo_memory::memory_store::MemorySplitPiece {
                        title: "Canonical database engine".to_string(),
                        r#type: Some(DurableMemoryType::Project),
                        content: "The database is PostgreSQL.".to_string(),
                        tags: vec!["database".to_string()],
                    },
                    bamboo_memory::memory_store::MemorySplitPiece {
                        title: "Canonical database persistence".to_string(),
                        r#type: Some(DurableMemoryType::Project),
                        content: "Canonical persistence relies on PostgreSQL.".to_string(),
                        tags: vec!["database".to_string()],
                    },
                ],
                Some("__memory_gardener__"),
                "memory-gardener",
            )
            .await
            .expect("split old AutoDream memory")
            .expect("gardener split result");
        let manual = memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Manual database note",
                "Keep the migration checklist available.",
                &["manual".to_string()],
                Some(&session.id),
                "user",
                false,
                None,
            )
            .await
            .expect("seed manual memory");
        memory
            .mark_session_extracted(&session.id, &old_watermark.to_rfc3339())
            .await
            .expect("seed extraction watermark");

        session.messages[0].content = "The database is SQLite.".to_string();
        session.messages[0].mark_content_updated_at(rewritten_at);
        session.clear_derived_context_state();
        session.updated_at = rewritten_at;
        storage
            .save_session(&session)
            .await
            .expect("save rewritten Session");

        let response = serde_json::json!({
            "candidates": [{
                "title": "Canonical database",
                "type": "project",
                "scope": "global",
                "content": "The database is SQLite.",
                "tags": ["database"],
                "session_id": "history-rewrite-memory"
            }],
            "ledger_candidates": [{
                "title": "Verify corrected database",
                "kind": "todo",
                "excerpt": "Confirm the SQLite migration.",
                "session_id": "history-rewrite-memory",
                "confidence": "high"
            }],
            "source_exhausted": true
        })
        .to_string();
        let ledger = LedgerStore::new(temp_dir.path());
        let blocker_path = ledger.resolver().records_dir(LedgerScope::Global, None);
        let sequence_provider = Arc::new(SequenceProvider::new(vec![response]));
        let provider: Arc<dyn LLMProvider> = Arc::new(PathBlockingProvider {
            inner: sequence_provider.as_ref().clone(),
            blocker_path: blocker_path.clone(),
        });
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let since = old_watermark - chrono::Duration::hours(1);
        let contexts = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(contexts.len(), 1);
        assert!(contexts[0].history_revision.is_some());
        assert!(contexts[0]
            .summary
            .as_deref()
            .is_some_and(|source| source.contains("The database is SQLite.")));
        assert!(!contexts[0]
            .summary
            .as_deref()
            .is_some_and(|source| source.contains("The database is PostgreSQL.")));

        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect_err("ledger failure must keep the frozen replacement plan retryable");

        let interim = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list memories after partial rewrite");
        assert_eq!(
            interim
                .iter()
                .filter(|document| document.frontmatter.status == DurableMemoryStatus::Active)
                .count(),
            4,
            "gardener descendants, manual, and replacement memories stay active until both sinks succeed"
        );
        let history_revision = contexts[0]
            .history_revision
            .as_deref()
            .expect("history revision");
        let plan_path =
            history_rewrite_plan_path(&context, "history-rewrite-memory", history_revision);
        let plan_json = tokio::fs::read_to_string(&plan_path)
            .await
            .expect("read frozen replacement plan");
        assert!(!plan_json.contains("PostgreSQL"));
        assert!(!plan_json.contains("SQLite"));

        let partial_replacement_id = interim
            .iter()
            .find(|document| {
                document.frontmatter.status == DurableMemoryStatus::Active
                    && document.body == "The database is SQLite."
            })
            .expect("partially persisted replacement stays active")
            .frontmatter
            .id
            .clone();
        let gardener_retry_descendants = memory
            .split_memory(
                &gardener_split.new_ids[0],
                None,
                &[
                    bamboo_memory::memory_store::MemorySplitPiece {
                        title: "Retry database engine lineage".to_string(),
                        r#type: Some(DurableMemoryType::Project),
                        content: "The database is PostgreSQL.".to_string(),
                        tags: vec!["database".to_string()],
                    },
                    bamboo_memory::memory_store::MemorySplitPiece {
                        title: "Retry database persistence lineage".to_string(),
                        r#type: Some(DurableMemoryType::Project),
                        content: "Canonical persistence still cites PostgreSQL.".to_string(),
                        tags: vec!["database".to_string()],
                    },
                ],
                Some("__memory_gardener__"),
                "memory-gardener",
            )
            .await
            .expect("advance gardener lineage after failed rewrite")
            .expect("gardener retry split result");
        let replacement_retry_descendants = memory
            .split_memory(
                &partial_replacement_id,
                None,
                &[
                    bamboo_memory::memory_store::MemorySplitPiece {
                        title: "Corrected SQLite engine".to_string(),
                        r#type: Some(DurableMemoryType::Project),
                        content: "The database is SQLite.".to_string(),
                        tags: vec!["database".to_string()],
                    },
                    bamboo_memory::memory_store::MemorySplitPiece {
                        title: "Corrected SQLite persistence".to_string(),
                        r#type: Some(DurableMemoryType::Project),
                        content: "Canonical persistence now uses SQLite.".to_string(),
                        tags: vec!["database".to_string()],
                    },
                ],
                Some("__memory_gardener__"),
                "memory-gardener",
            )
            .await
            .expect("advance partial replacement lineage after failed rewrite")
            .expect("replacement retry split result");

        tokio::fs::remove_file(&blocker_path)
            .await
            .expect("repair ledger fixture");
        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("checkpoint retry must complete the corrected rewrite");
        assert!(!plan_path.exists());

        let documents = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list reconciled memories");
        let old = documents
            .iter()
            .find(|document| document.frontmatter.id == old_auto.frontmatter.id)
            .expect("old AutoDream memory remains auditable");
        assert_eq!(old.frontmatter.status, DurableMemoryStatus::Superseded);
        for id in &gardener_split.new_ids {
            let descendant = documents
                .iter()
                .find(|document| document.frontmatter.id == *id)
                .expect("gardener descendant remains auditable");
            assert_eq!(
                descendant.frontmatter.status,
                DurableMemoryStatus::Superseded
            );
        }
        for id in &gardener_retry_descendants.new_ids {
            let descendant = documents
                .iter()
                .find(|document| document.frontmatter.id == *id)
                .expect("retry gardener descendant remains auditable");
            assert_eq!(
                descendant.frontmatter.status,
                DurableMemoryStatus::Superseded
            );
        }
        let partial_replacement = documents
            .iter()
            .find(|document| document.frontmatter.id == partial_replacement_id)
            .expect("partial replacement remains auditable");
        assert_eq!(
            partial_replacement.frontmatter.status,
            DurableMemoryStatus::Superseded,
            "the gardener transformation remains auditable"
        );
        for id in &replacement_retry_descendants.new_ids {
            let descendant = documents
                .iter()
                .find(|document| document.frontmatter.id == *id)
                .expect("replacement retry descendant remains auditable");
            assert_eq!(
                descendant.frontmatter.status,
                DurableMemoryStatus::Active,
                "retry revalidation must not reclassify a partially persisted replacement lineage as old"
            );
        }
        let manual = documents
            .iter()
            .find(|document| document.frontmatter.id == manual.frontmatter.id)
            .expect("manual memory remains present");
        assert_eq!(manual.frontmatter.status, DurableMemoryStatus::Active);
        assert!(documents.iter().any(|document| {
            document.frontmatter.status == DurableMemoryStatus::Active
                && document.body == "The database is SQLite."
        }));
        assert!(!documents.iter().any(|document| {
            document.frontmatter.status == DurableMemoryStatus::Active
                && document.body == "The database is PostgreSQL."
        }));
        assert_eq!(sequence_provider.recorded_prompts().len(), 1);
        assert_eq!(
            ledger
                .list_records(LedgerScope::Global, None, &RecordFilter::default())
                .await
                .expect("list retry ledger records")
                .len(),
            1
        );
        assert!(
            collect_candidate_session_contexts(&context, &memory, since)
                .await
                .is_empty(),
            "the same history generation must be acknowledged exactly once"
        );
    }

    #[tokio::test]
    async fn project_history_rewrite_freezes_global_and_project_memory_targets() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let project_id = ProjectId::parse("project-history-cross-scope").expect("project id");
        let project_key = project_id.to_string();
        let mut session = Session::new("project-history-cross-scope", "model");
        session.set_project_id_meta(project_key.clone());
        session.messages.push(Message::assistant(
            "The release train uses the old route.",
            None,
        ));
        session.clear_derived_context_state();
        storage.save_session(&session).await.expect("save Session");

        let base_memory = MemoryStore::new(temp_dir.path());
        let project_memory = base_memory.for_project(&project_id);
        let global = base_memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Global release route",
                "The release train uses the old global route.",
                &["release".to_string()],
                Some(&session.id),
                AUTO_DREAM_MEMORY_ACTOR,
                false,
                None,
            )
            .await
            .expect("seed Global AutoDream memory");
        let project = project_memory
            .write_memory(
                MemoryScope::Project,
                Some(&project_key),
                DurableMemoryType::Project,
                "Project release route",
                "The project release train uses the old route.",
                &["release".to_string()],
                Some(&session.id),
                AUTO_DREAM_MEMORY_ACTOR,
                false,
                None,
            )
            .await
            .expect("seed Project AutoDream memory");
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: base_memory,
            provider: Arc::new(SequenceProvider::new(Vec::<String>::new())),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let sessions = collect_candidate_sessions_for_project(
            &context,
            &project_key,
            Utc::now() - chrono::Duration::hours(1),
        )
        .await
        .expect("collect Project Session");
        let contexts =
            collect_candidate_session_contexts_from_sessions(&context, &project_memory, sessions)
                .await
                .expect("collect Project history rewrite");
        let rewritten = contexts
            .iter()
            .find(|context| context.history_revision.is_some())
            .expect("history-rewrite context");

        let targets = collect_history_rewrite_replacement_targets(
            &context,
            &project_memory,
            rewritten,
            None,
            true,
        )
        .await
        .expect("collect cross-scope replacement targets");
        assert!(targets
            .replacement_targets
            .contains(&MemoryReplacementTarget {
                id: global.frontmatter.id,
                scope: MemoryScope::Global,
                project_key: None,
            }));
        assert!(targets
            .replacement_targets
            .contains(&MemoryReplacementTarget {
                id: project.frontmatter.id,
                scope: MemoryScope::Project,
                project_key: Some(project_key),
            }));
    }

    #[tokio::test]
    async fn history_rewrite_preserves_unaffected_sources_from_gardener_consolidation() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let old_watermark = Utc::now() - chrono::Duration::minutes(1);
        let rewritten_at = old_watermark + chrono::Duration::seconds(10);

        let mut database_session = Session::new("mixed-lineage-database", "model");
        database_session.title = "Database choice".to_string();
        database_session.messages.push(message_at(
            Message::assistant("The database is PostgreSQL.", None),
            "assistant-database",
            old_watermark - chrono::Duration::seconds(5),
        ));
        database_session.updated_at = old_watermark;
        storage
            .save_session(&database_session)
            .await
            .expect("save database Session");

        let mut region_session = Session::new("mixed-lineage-region", "model");
        region_session.title = "Deployment region".to_string();
        region_session.messages.push(message_at(
            Message::assistant("The deployment region is eu-west-1.", None),
            "assistant-region",
            old_watermark - chrono::Duration::seconds(4),
        ));
        region_session.updated_at = old_watermark;
        storage
            .save_session(&region_session)
            .await
            .expect("save region Session");

        let memory = MemoryStore::new(temp_dir.path());
        let database_memory = memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Database engine",
                "The database is PostgreSQL.",
                &["database".to_string()],
                Some(&database_session.id),
                AUTO_DREAM_MEMORY_ACTOR,
                false,
                None,
            )
            .await
            .expect("seed database memory");
        let region_memory = memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Deployment region",
                "The deployment region is eu-west-1.",
                &["region".to_string()],
                Some(&region_session.id),
                AUTO_DREAM_MEMORY_ACTOR,
                false,
                None,
            )
            .await
            .expect("seed region memory");
        let manual_memory = memory
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Deployment runbook",
                "The manual rollback runbook remains authoritative.",
                &["runbook".to_string()],
                None,
                "user",
                false,
                None,
            )
            .await
            .expect("seed manual memory without a Session source");
        let consolidated = memory
            .consolidate_memories(
                &[
                    database_memory.frontmatter.id.clone(),
                    region_memory.frontmatter.id.clone(),
                    manual_memory.frontmatter.id.clone(),
                ],
                None,
                &bamboo_memory::memory_store::MemorySplitPiece {
                    title: "Deployment database and region".to_string(),
                    r#type: Some(DurableMemoryType::Project),
                    content: "The database is PostgreSQL, the deployment region is eu-west-1, and the manual rollback runbook remains authoritative."
                        .to_string(),
                    tags: vec![
                        "database".to_string(),
                        "region".to_string(),
                        "runbook".to_string(),
                    ],
                },
                Some("__memory_gardener__"),
                "memory-dedup-gardener",
            )
            .await
            .expect("consolidate memories")
            .expect("consolidated memory");
        memory
            .mark_session_extracted(&database_session.id, &old_watermark.to_rfc3339())
            .await
            .expect("seed database watermark");
        memory
            .mark_session_extracted(&region_session.id, &old_watermark.to_rfc3339())
            .await
            .expect("seed region watermark");

        database_session.messages[0].content = "The database is SQLite.".to_string();
        database_session.messages[0].mark_content_updated_at(rewritten_at);
        database_session.clear_derived_context_state();
        database_session.updated_at = rewritten_at;
        storage
            .save_session(&database_session)
            .await
            .expect("save rewritten database Session");

        let database_response = serde_json::json!({
            "candidates": [{
                "title": "Database engine",
                "type": "project",
                "scope": "global",
                "content": "The database is SQLite.",
                "tags": ["database"],
                "session_id": "mixed-lineage-database"
            }],
            "ledger_candidates": [],
            "source_exhausted": true
        })
        .to_string();
        let region_response = serde_json::json!({
            "candidates": [],
            "ledger_candidates": [],
            "source_exhausted": true
        })
        .to_string();
        let sequence_provider = Arc::new(SequenceProvider::new(vec![
            database_response,
            region_response,
        ]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let since = old_watermark - chrono::Duration::hours(1);
        let contexts = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(contexts.len(), 1, "only the rewritten Session is eligible");
        assert_eq!(contexts[0].session_id, database_session.id);

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
        .expect("rewrite and preservation transaction succeeds");
        assert_eq!(writes.memory, 1);
        assert_eq!(writes.ledger, 0);

        let documents = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list reconciled memories");
        let consolidated = documents
            .iter()
            .find(|document| document.frontmatter.id == consolidated.new_id)
            .expect("consolidated record remains auditable");
        assert_eq!(
            consolidated.frontmatter.status,
            DurableMemoryStatus::Superseded
        );
        let active_bodies = documents
            .iter()
            .filter(|document| document.frontmatter.status == DurableMemoryStatus::Active)
            .map(|document| document.body.as_str())
            .collect::<Vec<_>>();
        assert!(active_bodies.contains(&"The database is SQLite."));
        assert!(active_bodies.contains(&"The deployment region is eu-west-1."));
        assert!(active_bodies.contains(&"The manual rollback runbook remains authoritative."));
        assert!(!active_bodies.iter().any(|body| body.contains("PostgreSQL")));
        let manual_memory = documents
            .iter()
            .find(|document| document.frontmatter.id == manual_memory.frontmatter.id)
            .expect("manual ancestor remains auditable");
        assert_eq!(
            manual_memory.frontmatter.status,
            DurableMemoryStatus::Active
        );
        let region_memory = documents
            .iter()
            .find(|document| document.frontmatter.id == region_memory.frontmatter.id)
            .expect("unaffected Session ancestor remains auditable");
        assert_eq!(
            region_memory.frontmatter.status,
            DurableMemoryStatus::Active,
            "an empty preservation extraction must retain the exact unaffected lineage"
        );

        let region_state = memory
            .read_session_state(&region_session.id)
            .await
            .expect("read unaffected Session state");
        assert_eq!(
            region_state.last_extracted_at.as_deref(),
            Some(old_watermark.to_rfc3339().as_str()),
            "preservation must not advance the unaffected Session watermark"
        );
        assert!(
            ledger
                .list_records(LedgerScope::Global, None, &RecordFilter::default())
                .await
                .expect("list preservation ledger records")
                .is_empty(),
            "preservation batches cannot create prospective work"
        );
        let prompts = sequence_provider.recorded_prompts();
        assert_eq!(prompts.len(), 2);
        assert!(prompts[0].contains("The database is SQLite."));
        assert!(!prompts[0].contains("PostgreSQL"));
        assert!(prompts[1].contains("unaffected source"));
        assert!(prompts[1].contains("The deployment region is eu-west-1."));
    }

    #[tokio::test]
    async fn history_rewrite_replaces_suggested_ledger_records_from_the_old_transcript() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let old_watermark = Utc::now() - chrono::Duration::minutes(1);
        let rewritten_at = old_watermark + chrono::Duration::seconds(10);
        let mut session = Session::new("history-rewrite-ledger", "model");
        session.title = "Report commitment".to_string();
        session.messages.push(message_at(
            Message::user("I will submit the report on Friday."),
            "user-report",
            old_watermark - chrono::Duration::seconds(5),
        ));
        session.updated_at = old_watermark;
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .mark_session_extracted(&session.id, &old_watermark.to_rfc3339())
            .await
            .expect("seed extraction watermark");
        let ledger = LedgerStore::new(temp_dir.path());
        let old_record_id = "rec_history_rewrite_old".to_string();
        let mut old_record =
            LedgerRecord::new(old_record_id.clone(), RecordKind::Todo, "Submit the report");
        old_record.tags = vec!["suggested".to_string()];
        old_record.source.created_by = RecordActor::Extractor;
        old_record.source.session_id = Some(session.id.clone());
        old_record.source.excerpt = Some("I will submit the report on Friday.".to_string());
        ledger
            .write_record(old_record, None)
            .await
            .expect("seed suggested ledger record");

        session.messages[0].content = "I will submit the report on Monday.".to_string();
        session.messages[0].mark_content_updated_at(rewritten_at);
        session.clear_derived_context_state();
        session.updated_at = rewritten_at;
        storage
            .save_session(&session)
            .await
            .expect("save rewritten Session");

        let response = serde_json::json!({
            "candidates": [],
            "ledger_candidates": [{
                "title": "Submit the report",
                "kind": "todo",
                "excerpt": "I will submit the report on Monday.",
                "session_id": "history-rewrite-ledger",
                "confidence": "high"
            }],
            "source_exhausted": true
        })
        .to_string();
        let sequence_provider = Arc::new(SequenceProvider::new(vec![response]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let since = old_watermark - chrono::Duration::hours(1);
        let contexts = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].session_id, session.id);

        let writes = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &contexts,
        )
        .await
        .expect("ledger rewrite transaction succeeds");
        assert_eq!(writes.memory, 0);
        assert_eq!(writes.ledger, 1);

        let all_records = ledger
            .list_records(
                LedgerScope::Global,
                None,
                &RecordFilter {
                    include_terminal: true,
                    ..RecordFilter::default()
                },
            )
            .await
            .expect("list all ledger records");
        assert_eq!(all_records.len(), 2);
        let old_record = all_records
            .iter()
            .find(|document| document.record.id == old_record_id)
            .expect("old record remains auditable");
        assert_eq!(old_record.record.status, RecordStatus::Cancelled);
        assert!(old_record.record.transitions.iter().any(|transition| {
            transition.to_status == RecordStatus::Cancelled
                && transition
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("canonical Session history rewrite"))
        }));
        let current_records = ledger
            .list_records(LedgerScope::Global, None, &RecordFilter::default())
            .await
            .expect("list current ledger records");
        assert_eq!(current_records.len(), 1);
        assert_ne!(current_records[0].record.id, old_record_id);
        assert_eq!(current_records[0].record.title, "Submit the report");
        assert_eq!(current_records[0].record.status, RecordStatus::Open);
        assert_eq!(
            current_records[0].record.source.excerpt.as_deref(),
            Some("I will submit the report on Monday.")
        );
        assert_eq!(sequence_provider.recorded_prompts().len(), 1);
    }

    #[tokio::test]
    async fn failed_retrieval_extraction_keeps_the_same_delta_retryable() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let now = Utc::now();
        let mut session = Session::new("retrieval-retry", "model");
        session.title = "Retrieval retry".to_string();
        session
            .compression_events
            .push(retrieval_event("event", now - chrono::Duration::seconds(1)));
        let mut archived = message_at(
            Message::user("RETRYABLE_ARCHIVED_SOURCE"),
            "archived",
            now - chrono::Duration::minutes(1),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event".to_string());
        session.messages.push(archived);
        session.updated_at = now;
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "not valid extraction JSON".to_string(),
            serde_json::json!({"candidates": [], "ledger_candidates": []}).to_string(),
        ]));
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let since = now - chrono::Duration::hours(24);
        let first = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(first.len(), 1);
        let first_source = first[0].summary.clone();
        let ledger = LedgerStore::new(temp_dir.path());

        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &first,
        )
        .await
        .expect_err("malformed extraction output must fail before the watermark");
        let failed_state = memory
            .read_session_state("retrieval-retry")
            .await
            .expect("read failed extraction state");
        assert!(failed_state.last_extracted_at.is_none());

        let retry = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].summary, first_source);
        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &retry,
        )
        .await
        .expect("retry should accept the same bounded source");
        let completed_state = memory
            .read_session_state("retrieval-retry")
            .await
            .expect("read completed extraction state");
        assert_eq!(
            completed_state.last_extracted_at.as_deref(),
            Some(retry[0].entry.updated_at.to_rfc3339().as_str())
        );
    }

    #[tokio::test]
    async fn retrieval_extraction_advances_watermark_only_after_every_source_batch_succeeds() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let now = Utc::now();
        let mut session = Session::new("retrieval-multi-batch-retry", "model");
        session.title = "Retrieval multi-batch retry".to_string();
        session
            .compression_events
            .push(retrieval_event("event", now - chrono::Duration::seconds(1)));
        let oversized_source = format!(
            "MULTI_BATCH_SOURCE_START{}MULTI_BATCH_SOURCE_END",
            "x".repeat(10_000)
        );
        let mut archived = message_at(
            Message::user(oversized_source),
            "archived",
            now - chrono::Duration::minutes(1),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event".to_string());
        session.messages.push(archived);
        session.updated_at = now;
        storage.save_session(&session).await.expect("save Session");

        let first_prefix_response = serde_json::json!({
            "candidates": [{
                "title": "Prefix candidate before later failure",
                "type": "reference",
                "scope": "global",
                "content": "FIRST_WORDING_MUST_NOT_PERSIST",
                "tags": ["retry"],
                "session_id": "retrieval-multi-batch-retry"
            }],
            "ledger_candidates": []
        })
        .to_string();
        let retry_response = serde_json::json!({
            "candidates": [{
                "title": "Rephrased candidate after retry",
                "type": "reference",
                "scope": "global",
                "content": "SECOND_WORDING_PERSISTS_ONCE",
                "tags": ["retry"],
                "session_id": "retrieval-multi-batch-retry"
            }],
            "ledger_candidates": []
        })
        .to_string();
        let sequence_provider = Arc::new(SequenceProvider::new(vec![
            first_prefix_response,
            "not valid extraction JSON".to_string(),
            retry_response,
        ]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
        let memory = MemoryStore::new(temp_dir.path());
        let context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: provider.clone(),
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let since = now - chrono::Duration::hours(24);
        let first = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(first.len(), 2, "source must form two independent calls");
        assert!(first.iter().all(|context| {
            context
                .summary
                .as_ref()
                .is_some_and(|summary| summary.chars().count() <= RETRIEVAL_EXTRACTION_MAX_CHARS)
        }));
        assert!(first.iter().all(|context| context.topics.is_empty()));
        let first_sources = first
            .iter()
            .map(|context| context.summary.clone())
            .collect::<Vec<_>>();
        let ledger = LedgerStore::new(temp_dir.path());

        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &first,
        )
        .await
        .expect_err("a later provider batch failure must fail the complete extraction");
        assert!(
            memory
                .read_session_state("retrieval-multi-batch-retry")
                .await
                .expect("read failed extraction state")
                .last_extracted_at
                .is_none(),
            "a successful prefix must not advance the Session watermark"
        );
        assert!(
            memory
                .list_memory_documents(MemoryScope::Global, None)
                .await
                .expect("list memory after failed provider suffix")
                .is_empty(),
            "all provider batches must parse before a successful prefix can persist"
        );

        let retry = collect_candidate_session_contexts(&context, &memory, since).await;
        assert_eq!(
            retry
                .iter()
                .map(|context| context.summary.clone())
                .collect::<Vec<_>>(),
            first_sources,
            "every source batch must remain deterministic and retryable"
        );
        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &ledger,
            "fast-model",
            &retry,
        )
        .await
        .expect("all retry batches should succeed");
        let completed_state = memory
            .read_session_state("retrieval-multi-batch-retry")
            .await
            .expect("read completed extraction state");
        assert_eq!(
            completed_state.last_extracted_at.as_deref(),
            Some(retry[0].entry.updated_at.to_rfc3339().as_str())
        );
        let persisted = memory
            .list_memory_documents(MemoryScope::Global, None)
            .await
            .expect("list memory after complete retry");
        assert_eq!(persisted.len(), 2);
        assert!(persisted
            .iter()
            .any(|document| document.body == "FIRST_WORDING_MUST_NOT_PERSIST"));
        assert!(persisted
            .iter()
            .any(|document| document.body == "SECOND_WORDING_PERSISTS_ONCE"));

        let prompts = sequence_provider.recorded_prompts();
        assert_eq!(
            prompts.len(),
            3,
            "retry must reuse the successful first provider checkpoint"
        );
        assert!(prompts[0].contains("MULTI_BATCH_SOURCE_START"));
        assert!(prompts[1].contains("continuation_overlap_content_suffix"));
        assert!(prompts[1].contains("MULTI_BATCH_SOURCE_END"));
        assert!(prompts[2].contains("continuation_overlap_content_suffix"));
        assert!(prompts[2].contains("MULTI_BATCH_SOURCE_END"));
        assert!(
            collect_candidate_session_contexts(&context, &memory, since)
                .await
                .is_empty(),
            "a fully acknowledged source must not be extracted again"
        );
    }

    #[tokio::test]
    async fn saturated_retrieval_source_pages_before_advancing_the_watermark() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let now = Utc::now();
        let mut session = Session::new("retrieval-capacity", "model");
        session.title = "Retrieval capacity".to_string();
        session
            .compression_events
            .push(retrieval_event("event", now - chrono::Duration::seconds(1)));
        let facts = (0..9)
            .map(|index| format!("FACT_{index:03} is independently durable."))
            .collect::<Vec<_>>()
            .join(" ");
        let mut archived = message_at(
            Message::user(facts),
            "archived",
            now - chrono::Duration::minutes(1),
        );
        archived.compressed = true;
        archived.compressed_by_event_id = Some("event".to_string());
        session.messages.push(archived);
        session.updated_at = now;
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        let discovery_provider: Arc<dyn LLMProvider> =
            Arc::new(SequenceProvider::new(Vec::<String>::new()));
        let mut context = AutoDreamContext {
            session_store,
            storage,
            memory: memory.clone(),
            provider: discovery_provider,
            config: Arc::new(RwLock::new(Config::default())),
            provider_registry: test_registry(),
        };
        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            now - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);
        assert!(contexts[0]
            .summary
            .as_deref()
            .is_some_and(|source| source.contains("source_items_in_batch: 1")));
        let candidate = |index| {
            serde_json::json!({
                        "title": format!("Retrieved fact {index}"),
                        "type": "reference",
                        "scope": "global",
                        "content": format!("Durable retrieved fact number {index}."),
                        "tags": ["capacity"],
                        "session_id": "retrieval-capacity"
            })
        };
        let responses = vec![
            serde_json::json!({
                "candidates": (0..8).map(&candidate).collect::<Vec<_>>(),
                "ledger_candidates": [],
                "source_exhausted": false
            })
            .to_string(),
            serde_json::json!({
                "candidates": [candidate(8)],
                "ledger_candidates": [],
                "source_exhausted": true
            })
            .to_string(),
        ];
        let sequence_provider = Arc::new(SequenceProvider::new(responses));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
        context.provider = provider.clone();

        let writes = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &LedgerStore::new(temp_dir.path()),
            "fast-model",
            &contexts,
        )
        .await
        .expect("every retrieval source batch should persist");
        assert_eq!(writes.memory, 9);
        assert_eq!(sequence_provider.recorded_prompts().len(), 2);
        assert!(sequence_provider.recorded_prompts()[1].contains("Exhaustive continuation page 2"));
        assert!(sequence_provider.recorded_prompts()[1].contains("Retrieved fact 0"));
        assert_eq!(
            memory
                .list_memory_documents(MemoryScope::Global, None)
                .await
                .expect("list extracted memories")
                .len(),
            9
        );
        assert_eq!(
            memory
                .read_session_state("retrieval-capacity")
                .await
                .expect("read extraction watermark")
                .last_extracted_at
                .as_deref(),
            Some(contexts[0].entry.updated_at.to_rfc3339().as_str())
        );
    }

    #[tokio::test]
    async fn durable_checkpoint_rebuilds_retrieval_delta_after_replay() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let mut session = Session::new("sink-retry", "model");
        session.title = "Sink retry".to_string();
        let first_source_watermark = Utc::now() - chrono::Duration::minutes(1);
        let first_event = retrieval_event(
            "sink-retry-event-1",
            first_source_watermark - chrono::Duration::seconds(1),
        );
        let mut old_message = message_at(
            Message::user("OLD_RETRY_SOURCE must be checkpointed exactly once."),
            "sink-retry-old-message",
            first_source_watermark - chrono::Duration::seconds(2),
        );
        old_message.compressed = true;
        old_message.compressed_by_event_id = Some(first_event.id.clone());
        session.compression_events.push(first_event);
        session.messages.push(old_message);
        session.updated_at = first_source_watermark;
        storage.save_session(&session).await.expect("save Session");

        let response = serde_json::json!({
            "candidates": [{
                "title": "Durable retry fact",
                "type": "reference",
                "scope": "global",
                "content": "The durable retry fact must be written exactly once.",
                "tags": ["retry"],
                "session_id": "sink-retry"
            }],
            "ledger_candidates": [{
                "title": "Verify durable retry",
                "kind": "todo",
                "due_at": null,
                "starts_at": null,
                "excerpt": "Remember the retry fact and remind me to verify it.",
                "session_id": "sink-retry",
                "confidence": "high"
            }]
        })
        .to_string();
        let newer_response = serde_json::json!({
            "candidates": [{
                "title": "Newer retry fact",
                "type": "reference",
                "scope": "global",
                "content": "The newer source update must be extracted after the old checkpoint replay.",
                "tags": ["retry"],
                "session_id": "sink-retry"
            }],
            "ledger_candidates": []
        })
        .to_string();
        let newer_topic_response = serde_json::json!({
            "candidates": [{
                "title": "Newer retry topic",
                "type": "reference",
                "scope": "global",
                "content": "The Session topic added after the old checkpoint must be extracted.",
                "tags": ["retry", "topic"],
                "session_id": "sink-retry"
            }],
            "ledger_candidates": []
        })
        .to_string();
        let sequence_provider = Arc::new(SequenceProvider::new(vec![
            response,
            newer_response,
            newer_topic_response,
        ]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
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
        assert!(contexts[0]
            .summary
            .as_deref()
            .is_some_and(|source| source.contains("OLD_RETRY_SOURCE")));

        tokio::fs::write(temp_dir.path().join("ledger"), b"blocks ledger directory")
            .await
            .expect("create ledger failure fixture");
        extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &LedgerStore::new(temp_dir.path()),
            "fast-model",
            &contexts,
        )
        .await
        .expect_err("the ledger sink must fail after the memory write");
        assert_eq!(sequence_provider.recorded_prompts().len(), 1);
        assert_eq!(
            memory
                .list_memory_documents(MemoryScope::Global, None)
                .await
                .expect("list memory after sink failure")
                .len(),
            1
        );
        assert!(memory
            .read_session_state("sink-retry")
            .await
            .expect("read failed watermark")
            .last_extracted_at
            .is_none());

        let retrieval_source_key = contexts[0]
            .retrieval_source_key
            .as_deref()
            .expect("retrieval context has a source identity");
        write_retrieval_source_state(
            &context,
            "sink-retry",
            retrieval_source_key,
            &contexts[0].entry.updated_at.to_rfc3339(),
        )
        .await
        .expect("simulate marker success before Jiandu watermark failure");

        let newer_source_watermark = first_source_watermark + chrono::Duration::seconds(1);
        session.compression_events.push(retrieval_event(
            "sink-retry-event-2",
            newer_source_watermark,
        ));
        session.messages.push(message_at(
            Message::user("NEWER_RETRY_SOURCE must remain eligible after replay."),
            "sink-retry-new-message",
            newer_source_watermark,
        ));
        session.updated_at = newer_source_watermark;
        context
            .storage
            .save_session(&session)
            .await
            .expect("save newer retrieval source");
        memory
            .write_session_topic(
                "sink-retry",
                "continuity",
                "NEWER_RETRY_TOPIC must remain eligible after replay.",
            )
            .await
            .expect("write newer Session topic");
        let updated_contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(updated_contexts.len(), 2);
        let stale_transition_source = updated_contexts
            .iter()
            .find_map(|context| context.summary.as_deref())
            .expect("pre-replay retrieval transition source");
        assert!(stale_transition_source.contains("OLD_RETRY_SOURCE"));
        assert!(stale_transition_source.contains("NEWER_RETRY_SOURCE"));

        tokio::fs::remove_file(temp_dir.path().join("ledger"))
            .await
            .expect("repair ledger fixture");
        let writes = extract_and_persist_durable_candidates(
            &context,
            &provider,
            &memory,
            &LedgerStore::new(temp_dir.path()),
            "fast-model",
            &updated_contexts,
        )
        .await
        .expect("retry should replay the old checkpoint before extracting newer source");
        assert_eq!(writes.memory, 2);
        assert_eq!(writes.ledger, 1);
        let recorded_prompts = sequence_provider.recorded_prompts();
        assert_eq!(recorded_prompts.len(), 3);
        assert!(recorded_prompts[1].contains("NEWER_RETRY_SOURCE"));
        assert!(!recorded_prompts[1].contains("OLD_RETRY_SOURCE"));
        assert!(recorded_prompts[2].contains("NEWER_RETRY_TOPIC"));
        assert!(!recorded_prompts[2].contains("OLD_RETRY_SOURCE"));
        assert_eq!(
            memory
                .read_session_state("sink-retry")
                .await
                .expect("read committed watermark")
                .last_extracted_at
                .as_deref(),
            Some(updated_contexts[0].entry.updated_at.to_rfc3339().as_str()),
            "the old transaction and the newer source must both commit in order"
        );
        let (pending_transactions, pending_ids) =
            load_extraction_checkpoint_transactions(&context, "sink-retry")
                .await
                .expect("inspect acknowledged checkpoint directory");
        assert!(pending_transactions.is_empty());
        assert!(pending_ids.is_empty());
        assert_eq!(
            memory
                .list_memory_documents(MemoryScope::Global, None)
                .await
                .expect("list memory after retry")
                .len(),
            3
        );
        assert_eq!(
            LedgerStore::new(temp_dir.path())
                .list_records(LedgerScope::Global, None, &RecordFilter::default())
                .await
                .expect("list ledger after retry")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn checkpoint_replay_rebuilds_topic_only_delta_and_skips_unchanged_topics() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let acknowledged_watermark = Utc::now() - chrono::Duration::minutes(1);
        let mut session = Session::new("topic-only-replay", "model");
        session.title = "Topic only replay".to_string();
        session.compression_events.push(retrieval_event(
            "topic-only-boundary",
            acknowledged_watermark,
        ));
        session.updated_at = acknowledged_watermark + chrono::Duration::seconds(1);
        storage.save_session(&session).await.expect("save Session");

        let memory = MemoryStore::new(temp_dir.path());
        memory
            .write_session_topic(
                "topic-only-replay",
                "continuity",
                "TOPIC_ONLY_AFTER_CHECKPOINT must not be skipped.",
            )
            .await
            .expect("write Session topic");
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
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        let template = contexts.first().expect("topic context");

        let rebuilt = rebuild_session_contexts_after_checkpoint_replay(
            &context,
            template,
            acknowledged_watermark,
            Some(&extraction_topics_fingerprint(&[])),
        )
        .await
        .expect("rebuild topic-only delta");
        assert_eq!(rebuilt.contexts.len(), 1);
        assert!(rebuilt.contexts[0].summary.is_none());
        assert_eq!(rebuilt.contexts[0].topics.len(), 1);
        assert!(extraction_prompt(&rebuilt.contexts[0]).contains("TOPIC_ONLY_AFTER_CHECKPOINT"));

        let current_fingerprint = rebuilt.topics_fingerprint;
        let unchanged = rebuild_session_contexts_after_checkpoint_replay(
            &context,
            template,
            acknowledged_watermark,
            Some(&current_fingerprint),
        )
        .await
        .expect("skip unchanged topics");
        assert!(unchanged.contexts.is_empty());
        assert_eq!(unchanged.topics_fingerprint, current_fingerprint);
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
                    "session_id": "session-auto",
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
        assert_eq!(writes.ledger, 0);
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
        let project_id = ProjectId::parse("project-auto-dream").expect("project id");
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
        let provider: Arc<dyn LLMProvider> = Arc::new(SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"First Project fact\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"The first stable Project fact.\",\"tags\":[\"project\"],\"session_id\":\"session-assigned\"}]}".to_string(),
            "{\"candidates\":[{\"title\":\"Second Project fact\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"The second stable Project fact after switching workspaces.\",\"tags\":[\"project\"],\"session_id\":\"session-assigned\"}]}".to_string(),
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
    async fn run_auto_dream_once_updates_dream_and_persists_candidates() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        let provider = SequenceProvider::new(vec![
            "{\"candidates\":[{\"title\":\"User prefers concise answers\",\"type\":\"feedback\",\"scope\":\"project\",\"content\":\"The user prefers concise answers and minimal recap.\",\"tags\":[\"preference\"],\"session_id\":\"session-dream-run\"}],\"ledger_candidates\":[{\"title\":\"Renew passport\",\"kind\":\"todo\",\"due_at\":\"2026-08-01T00:00:00Z\",\"starts_at\":null,\"excerpt\":\"I need to renew my passport before August\",\"session_id\":\"session-dream-run\",\"confidence\":\"high\"}]}".to_string(),
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

        let writes = persist_ledger_candidates(&ledger, candidates, &HashSet::new())
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

        let writes = persist_ledger_candidates(&ledger, candidates, &HashSet::new())
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
            "{\"candidates\":[{\"title\":\"Project A prefers concise planning\",\"type\":\"project\",\"scope\":\"project\",\"content\":\"Project A plans should stay concise and scoped.\",\"tags\":[\"planning\"],\"session_id\":\"session-project-a\"}]}".to_string(),
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
                "{\"candidates\":[{\"title\":\"Persist once across CAS retry\",\"type\":\"feedback\",\"scope\":\"global\",\"content\":\"This durable fact must not be duplicated when Dream publication retries.\",\"tags\":[\"cas\"],\"session_id\":\"session-cas-dream\"}]}".to_string(),
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
    async fn candidate_session_load_failure_aborts_dream_cursor_and_remains_retryable() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let session_store = Arc::new(
            SessionStoreV2::new(temp_dir.path().to_path_buf())
                .await
                .expect("session store"),
        );
        let canonical_storage: Arc<dyn Storage> = session_store.clone();
        let mut session = Session::new("dream-load-retry", "model");
        session.title = "Dream load retry".to_string();
        session.conversation_summary = Some(bamboo_agent_core::ConversationSummary::new(
            "A durable fact must survive a transient Session read failure.",
            2,
            80,
        ));
        session.add_message(Message::user("Remember the retry boundary."));
        canonical_storage
            .save_session(&session)
            .await
            .expect("save candidate Session");

        let fail_loads = Arc::new(AtomicBool::new(true));
        let storage: Arc<dyn Storage> = Arc::new(ToggleLoadStorage {
            inner: canonical_storage,
            fail_loads: fail_loads.clone(),
        });
        let sequence_provider = Arc::new(SequenceProvider::new(vec![
            serde_json::json!({
                "candidates": [],
                "ledger_candidates": [],
                "source_exhausted": true
            })
            .to_string(),
            "## Current durable context\n- Retry completed\n\n## Cross-session patterns\n- None\n\n## Active threads to remember\n- None\n\n## Stable constraints and preferences\n- Preserve failed candidates\n\n## Open risks or questions\n- None".to_string(),
        ]));
        let provider: Arc<dyn LLMProvider> = sequence_provider.clone();
        let memory = MemoryStore::new(temp_dir.path());
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
            .expect_err("a candidate Session read failure must abort the whole Dream run");
        assert!(
            error.contains("failed to load canonical AutoDream extraction Session"),
            "{error}"
        );
        assert!(sequence_provider.recorded_prompts().is_empty());
        assert!(
            read_test_dream(&memory, MemoryScope::Global, None)
                .await
                .is_none(),
            "a failed candidate read must not publish a later consolidation cursor"
        );

        fail_loads.store(false, Ordering::SeqCst);
        let result = run_auto_dream_once_with_store(&context, &memory)
            .await
            .expect("the unchanged candidate remains retryable")
            .expect("the retry publishes Dream");
        assert_eq!(result.session_count, 1);
        assert_eq!(sequence_provider.recorded_prompts().len(), 2);
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
