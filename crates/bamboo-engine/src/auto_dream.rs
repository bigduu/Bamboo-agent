use std::cmp::Reverse;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::RwLock;

use bamboo_agent_core::{Message, SessionKind};
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_infrastructure::Config;
use bamboo_infrastructure::{LLMChunk, LLMProvider, LLMRequestOptions};
use bamboo_infrastructure::{ProviderModelRouter, ProviderRegistry};
use bamboo_infrastructure::{SessionIndexEntry, SessionStoreV2};
use bamboo_memory::auto_dream::{
    build_consolidation_prompt, build_extraction_prompt, build_rebuild_consolidation_prompt,
    build_refine_consolidation_prompt, derive_session_outline, normalize_dream_notebook_body,
    normalize_existing_dream_for_prompt, parse_candidate_scope, parse_candidate_type,
    parse_extraction_candidates, parse_last_consolidated_at, parse_last_full_rebuild_at,
    should_force_full_rebuild, should_use_dream_refine_mode, truncate_chars,
    ConsolidationSessionInfo, DreamCandidateInfo, DreamGenerationMode,
};
use bamboo_memory::memory_store::{MemoryScope, MemoryStore};

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
    pub provider_registry: Arc<ProviderRegistry>,
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
        .map(|path| bamboo_memory::memory_store::project_key_from_path(&path))
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
    provider: &Arc<dyn LLMProvider>,
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
    let raw = collect_stream_text(provider.clone(), model, prompt).await?;
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
    provider: &Arc<dyn LLMProvider>,
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
            match collect_stream_text(provider.clone(), model, refine_prompt).await {
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
                                collect_stream_text(provider.clone(), model, prompt).await?;
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
                    let raw_body = collect_stream_text(provider.clone(), model, prompt).await?;
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

    // Resolve background model (and provider when using ProviderModelRef).
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
        build_dream_notebook_body(&bg_provider, &model, &source_window, generation_mode).await?;
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
        extract_and_persist_durable_candidates(&bg_provider, memory, &model, &extraction_sessions)
            .await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use futures::stream;

    use bamboo_agent_core::storage::Storage;
    use bamboo_infrastructure::{LLMError, LLMStream};

    fn test_registry() -> Arc<ProviderRegistry> {
        Arc::new(ProviderRegistry::new(HashMap::new(), "test".to_string()))
    }

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
            provider_registry: test_registry(),
        };
        let contexts = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        assert_eq!(contexts.len(), 1);

        let writes =
            extract_and_persist_durable_candidates(&provider, &memory, "fast-model", &contexts)
                .await
                .expect("extraction should succeed");
        assert_eq!(writes, 1);

        let project_key = bamboo_memory::memory_store::project_key_from_path(
            &temp_dir.path().join("workspace-a"),
        );
        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key),
                Some("terse recap"),
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
            provider_registry: test_registry(),
        };
        let sessions = collect_candidate_session_contexts(
            &context,
            &memory,
            Utc::now() - chrono::Duration::hours(24),
        )
        .await;
        let writes = extract_and_persist_durable_candidates(
            &context.provider,
            &memory,
            "fast-model",
            &sessions,
        )
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
            provider_registry: test_registry(),
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

        let project_key = bamboo_memory::memory_store::project_key_from_path(
            &temp_dir.path().join("workspace-run"),
        );
        let results = memory
            .query_scope(
                MemoryScope::Project,
                Some(&project_key),
                Some("concise answers"),
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
    }

    #[tokio::test]
    async fn run_project_auto_dream_once_filters_sessions_by_project_and_writes_project_dream() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace_a = temp_dir.path().join("workspace-a");
        let workspace_b = temp_dir.path().join("workspace-b");
        std::fs::create_dir_all(&workspace_a).expect("workspace a");
        std::fs::create_dir_all(&workspace_b).expect("workspace b");
        let project_key_a = bamboo_memory::memory_store::project_key_from_path(&workspace_a);

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
            provider_registry: test_registry(),
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
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());

        let workspace_other = temp_dir.path().join("workspace-other");
        let workspace_target = temp_dir.path().join("workspace-target");
        std::fs::create_dir_all(&workspace_other).expect("workspace other");
        std::fs::create_dir_all(&workspace_target).expect("workspace target");
        let target_project_key =
            bamboo_memory::memory_store::project_key_from_path(&workspace_target);

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
            provider_registry: test_registry(),
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
        let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

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
            provider_registry: test_registry(),
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
            provider_registry: test_registry(),
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
        let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

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
                bamboo_memory::memory_store::DurableMemoryType::Project,
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
            provider_registry: test_registry(),
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
        let project_key = bamboo_memory::memory_store::project_key_from_path(&workspace);

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
                bamboo_memory::memory_store::DurableMemoryType::Project,
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
            provider_registry: test_registry(),
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
            provider_registry: test_registry(),
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
            provider_registry: test_registry(),
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
            provider_registry: test_registry(),
        };
        let result = run_auto_dream_once(&context)
            .await
            .expect("no candidate sessions should not error");
        assert!(result.is_none());
    }
}
