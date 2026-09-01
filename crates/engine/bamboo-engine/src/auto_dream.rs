use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::RwLock;

use bamboo_agent_core::{Message, SessionKind};
use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordActor, RecordKind};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_llm::Config;
use bamboo_llm::{LLMChunk, LLMProvider, LLMRequestOptions};
use bamboo_llm::{ProviderModelRouter, ProviderRegistry};
use bamboo_memory::auto_dream::{
    build_consolidation_prompt, build_extraction_prompt, build_rebuild_consolidation_prompt,
    derive_session_outline, normalize_dream_notebook_body, parse_candidate_scope,
    parse_candidate_type, parse_extraction_candidates, parse_last_consolidated_at,
    parse_last_full_rebuild_at, parse_ledger_candidates, should_force_full_rebuild, truncate_chars,
    ConsolidationSessionInfo, DreamCandidateInfo, DreamGenerationMode, LedgerExtractionCandidate,
};
use bamboo_memory::ledger_store::store::new_record_id;
use bamboo_memory::ledger_store::{LedgerStore, RecordFilter, MAX_RECORD_TITLE_LEN};
use bamboo_memory::memory_store::{
    DurableMemoryStatus, DurableMemoryType, MemoryScope, MemoryStore, MAX_MEMORY_TITLE_LEN,
};
use bamboo_storage::{SessionIndexEntry, SessionStoreV2};

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
}

#[derive(Debug, Clone)]
struct DreamSourceWindow {
    existing_dream: Option<String>,
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
    for (entry, summary) in sessions {
        let already_extracted = match memory.read_session_state(&entry.id).await {
            Ok(state) => state
                .last_extracted_at
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .is_some_and(|extracted_at| extracted_at >= entry.updated_at),
            Err(error) => {
                tracing::warn!(
                    target: DREAM_TRACING_TARGET,
                    event = "session_extraction_state_read_failed",
                    session_id = %entry.id,
                    "Could not read Jiandu session extraction state; keeping the session retryable: {error}"
                );
                false
            }
        };
        if already_extracted {
            continue;
        }
        let project_key = resolve_session_project_id(ctx, &entry.id)
            .await
            .map(bamboo_domain::ProjectId::into_string);
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
    let raw = collect_stream_text(provider.clone(), model, prompt).await?;
    let candidates = parse_extraction_candidates(&raw)?;
    // Tolerant by design: absent/malformed ledger array → empty vec.
    let ledger_candidates = parse_ledger_candidates(&raw);

    let mut session_project_keys = HashMap::new();
    for session in sessions {
        session_project_keys.insert(session.session_id.clone(), session.project_key.clone());
    }

    let mut writes = 0usize;
    let session_source_updated_at = sessions
        .iter()
        .map(|session| {
            (
                session.session_id.clone(),
                session.entry.updated_at.to_rfc3339(),
            )
        })
        .collect::<HashMap<_, _>>();
    type ExtractionFingerprint = (DurableMemoryType, String, String, String);
    let mut existing_by_scope: HashMap<
        (MemoryScope, Option<String>),
        HashSet<ExtractionFingerprint>,
    > = HashMap::new();
    for candidate in candidates.into_iter().take(EXTRACTION_MAX_CANDIDATES) {
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

    // Extraction completion is acknowledged only after both durable sinks have
    // accepted the batch. Store the source session's captured update watermark,
    // not the later wall-clock completion time: if the session changes while the
    // model is extracting, its newer index timestamp must remain retryable.
    for session in sessions {
        let session_id = &session.session_id;
        let source_updated_at = session_source_updated_at
            .get(session_id)
            .expect("a touched candidate must come from the extraction input");
        memory
            .mark_session_extracted(session_id, source_updated_at)
            .await
            .map_err(|error| {
                format!("failed to update session extraction state for {session_id}: {error}")
            })?;
    }

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
