use std::collections::{BTreeMap, BTreeSet};
use std::io;

use actix_web::{web, HttpResponse, Responder};
use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, Weekday};

use super::super::{
    internal_error, MemoryMetricsQuery, MemoryMetricsSummary, MemoryTimelinePoint,
    PromptMemoryMetricsSummary,
};
use crate::app_state::AppState;
use crate::handlers::agent::metrics::core_handlers::filters::{
    normalize_days, resolve_timeline_granularity, TimelineGranularity,
};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::PromptMemoryObservability;
use bamboo_memory::memory_store::{
    DurableMemoryDocument, MemoryInspectResult, MemoryScope, MemoryStore,
};
use bamboo_storage::SessionStoreV2;

fn merge_breakdown(target: &mut BTreeMap<String, u64>, source: &BTreeMap<String, usize>) {
    for (label, count) in source {
        *target.entry(label.clone()).or_insert(0) += *count as u64;
    }
}

fn update_latest_timestamp(target: &mut Option<DateTime<FixedOffset>>, value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(value) else {
        return;
    };

    match target {
        Some(current) if *current >= parsed => {}
        _ => *target = Some(parsed),
    }
}

async fn collect_prompt_memory_metrics(
    session_store: &SessionStoreV2,
    storage: &dyn Storage,
) -> io::Result<Option<PromptMemoryMetricsSummary>> {
    let entries = session_store.list_index_entries().await;
    let mut observed_sessions = 0_u64;
    let mut project_memory_index_hits = 0_u64;
    let mut relevant_memory_hits = 0_u64;
    let mut relevant_memory_reranked_hits = 0_u64;
    let mut relevant_memory_rerank_enabled_sessions = 0_u64;
    let mut relevant_memory_rerank_fallbacks = 0_u64;
    let mut global_dream_fallback_hits = 0_u64;
    let mut project_dream_hits = 0_u64;
    let mut context_pressure_warning_hits = 0_u64;
    let mut total_relevant_memory_count = 0_u64;
    let mut total_relevant_memory_section_chars = 0_u64;
    let mut total_external_memory_section_chars = 0_u64;
    let mut relevant_memory_status_breakdown = BTreeMap::new();
    let mut dream_source_breakdown = BTreeMap::new();
    let mut resolved_scope_breakdown = BTreeMap::new();

    for entry in entries {
        let Some(session) = storage.load_session(&entry.id).await? else {
            continue;
        };
        let Some(raw) = session.metadata.get("runtime_prompt_memory_observability") else {
            continue;
        };
        let Ok(observability) = serde_json::from_str::<PromptMemoryObservability>(raw) else {
            continue;
        };

        observed_sessions += 1;
        if matches!(
            observability.project_memory_index_status.as_str(),
            "loaded" | "loaded_truncated"
        ) {
            project_memory_index_hits += 1;
        }
        if observability.relevant_memory_count > 0 {
            relevant_memory_hits += 1;
        }
        if observability.relevant_recall_rerank_enabled {
            relevant_memory_rerank_enabled_sessions += 1;
        }
        if observability.relevant_memory_status == "reranked" {
            relevant_memory_reranked_hits += 1;
        }
        if observability.relevant_memory_status == "rerank_fallback" {
            relevant_memory_rerank_fallbacks += 1;
        }
        if observability.dream_source == "global_fallback" {
            global_dream_fallback_hits += 1;
        }
        if observability.dream_source == "project" {
            project_dream_hits += 1;
        }
        if observability.context_pressure_warning_chars > 0 {
            context_pressure_warning_hits += 1;
        }
        total_relevant_memory_count += observability.relevant_memory_count as u64;
        total_relevant_memory_section_chars += observability.relevant_memory_section_chars as u64;
        total_external_memory_section_chars += observability.external_memory_section_chars as u64;
        *relevant_memory_status_breakdown
            .entry(observability.relevant_memory_status.clone())
            .or_insert(0) += 1;
        *dream_source_breakdown
            .entry(observability.dream_source.clone())
            .or_insert(0) += 1;
        let resolved_scope = if observability.resolved_project_key.is_some() {
            "project".to_string()
        } else {
            "global_or_unscoped".to_string()
        };
        *resolved_scope_breakdown.entry(resolved_scope).or_insert(0) += 1;
    }

    if observed_sessions == 0 {
        return Ok(None);
    }

    Ok(Some(PromptMemoryMetricsSummary {
        observed_sessions,
        project_memory_index_hits,
        relevant_memory_hits,
        relevant_memory_reranked_hits,
        relevant_memory_rerank_enabled_sessions,
        relevant_memory_rerank_fallbacks,
        global_dream_fallback_hits,
        project_dream_hits,
        context_pressure_warning_hits,
        total_relevant_memory_count,
        avg_relevant_memory_count: total_relevant_memory_count / observed_sessions.max(1),
        avg_relevant_memory_section_chars: total_relevant_memory_section_chars
            / observed_sessions.max(1),
        avg_external_memory_section_chars: total_external_memory_section_chars
            / observed_sessions.max(1),
        relevant_memory_status_breakdown,
        dream_source_breakdown,
        resolved_scope_breakdown,
    }))
}

fn summarize_memory_results(
    results: &[MemoryInspectResult],
    scope: Option<MemoryScope>,
    project_key: Option<String>,
    project_count: u64,
    prompt_memory: Option<PromptMemoryMetricsSummary>,
) -> MemoryMetricsSummary {
    let mut total_memories = 0_u64;
    let mut stale_candidate_count = 0_u64;
    let mut by_type = BTreeMap::new();
    let mut by_status = BTreeMap::new();
    let mut by_scope = BTreeMap::new();
    let mut last_reindex_at = None;
    let mut last_dream_at = None;

    for result in results {
        total_memories += result.total_memories as u64;
        stale_candidate_count += result.stale_candidate_count as u64;
        merge_breakdown(&mut by_type, &result.by_type);
        merge_breakdown(&mut by_status, &result.by_status);
        *by_scope
            .entry(result.scope.as_str().to_string())
            .or_insert(0) += result.total_memories as u64;
        update_latest_timestamp(&mut last_reindex_at, result.last_reindex_at.as_deref());
        update_latest_timestamp(&mut last_dream_at, result.last_dream_at.as_deref());
    }

    MemoryMetricsSummary {
        scope,
        project_key,
        total_memories,
        stale_candidate_count,
        project_count,
        by_type,
        by_status,
        by_scope,
        last_reindex_at: last_reindex_at.map(|value| value.to_rfc3339()),
        last_dream_at: last_dream_at.map(|value| value.to_rfc3339()),
        prompt_memory,
    }
}

pub(crate) async fn build_memory_summary(
    store: &MemoryStore,
    session_store: &SessionStoreV2,
    storage: &dyn Storage,
    query: &MemoryMetricsQuery,
) -> io::Result<MemoryMetricsSummary> {
    let prompt_memory = collect_prompt_memory_metrics(session_store, storage).await?;
    let normalized_project_key = query
        .project_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let project_keys = store.list_project_keys().await?;

    match query.scope {
        Some(MemoryScope::Session) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session scope is not supported for memory metrics summary",
        )),
        Some(MemoryScope::Global) => {
            let result = store.inspect_scope(MemoryScope::Global, None).await?;
            Ok(summarize_memory_results(
                &[result],
                Some(MemoryScope::Global),
                None,
                0,
                prompt_memory,
            ))
        }
        Some(MemoryScope::Project) => {
            if let Some(project_key) = normalized_project_key {
                let result = store
                    .inspect_scope(MemoryScope::Project, Some(project_key.as_str()))
                    .await?;
                let project_count = if project_keys.iter().any(|value| value == &project_key) {
                    1
                } else {
                    0
                };
                Ok(summarize_memory_results(
                    &[result],
                    Some(MemoryScope::Project),
                    Some(project_key),
                    project_count,
                    prompt_memory,
                ))
            } else {
                let mut results = Vec::new();
                for project_key in &project_keys {
                    results.push(
                        store
                            .inspect_scope(MemoryScope::Project, Some(project_key.as_str()))
                            .await?,
                    );
                }
                Ok(summarize_memory_results(
                    &results,
                    Some(MemoryScope::Project),
                    None,
                    project_keys.len() as u64,
                    prompt_memory,
                ))
            }
        }
        None => {
            let mut results = Vec::with_capacity(project_keys.len() + 1);
            results.push(store.inspect_scope(MemoryScope::Global, None).await?);
            for project_key in &project_keys {
                results.push(
                    store
                        .inspect_scope(MemoryScope::Project, Some(project_key.as_str()))
                        .await?,
                );
            }
            Ok(summarize_memory_results(
                &results,
                None,
                None,
                project_keys.len() as u64,
                prompt_memory,
            ))
        }
    }
}

fn parse_memory_date(value: &str) -> Option<NaiveDate> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.date_naive())
}

async fn collect_memory_documents(
    store: &MemoryStore,
    query: &MemoryMetricsQuery,
) -> io::Result<Vec<DurableMemoryDocument>> {
    let normalized_project_key = query
        .project_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let project_keys = store.list_project_keys().await?;

    match query.scope {
        Some(MemoryScope::Session) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session scope is not supported for memory metrics timeline",
        )),
        Some(MemoryScope::Global) => store.list_memory_documents(MemoryScope::Global, None).await,
        Some(MemoryScope::Project) => {
            if let Some(project_key) = normalized_project_key {
                store
                    .list_memory_documents(MemoryScope::Project, Some(project_key.as_str()))
                    .await
            } else {
                let mut docs = Vec::new();
                for project_key in &project_keys {
                    docs.extend(
                        store
                            .list_memory_documents(MemoryScope::Project, Some(project_key.as_str()))
                            .await?,
                    );
                }
                Ok(docs)
            }
        }
        None => {
            let mut docs = store
                .list_memory_documents(MemoryScope::Global, None)
                .await?;
            for project_key in &project_keys {
                docs.extend(
                    store
                        .list_memory_documents(MemoryScope::Project, Some(project_key.as_str()))
                        .await?,
                );
            }
            Ok(docs)
        }
    }
}

fn start_of_week(date: NaiveDate) -> NaiveDate {
    let weekday = match date.weekday() {
        Weekday::Mon => 0,
        Weekday::Tue => 1,
        Weekday::Wed => 2,
        Weekday::Thu => 3,
        Weekday::Fri => 4,
        Weekday::Sat => 5,
        Weekday::Sun => 6,
    };

    date - Duration::days(weekday)
}

fn period_start(date: NaiveDate, granularity: TimelineGranularity) -> NaiveDate {
    match granularity {
        TimelineGranularity::Daily => date,
        TimelineGranularity::Weekly => start_of_week(date),
        TimelineGranularity::Monthly => date.with_day(1).unwrap_or(date),
    }
}

fn period_label(start: NaiveDate, end: NaiveDate) -> String {
    if start == end {
        start.to_string()
    } else {
        format!("{}..{}", start, end)
    }
}

pub(crate) async fn build_memory_timeline(
    store: &MemoryStore,
    query: &MemoryMetricsQuery,
) -> io::Result<Vec<MemoryTimelinePoint>> {
    let docs = collect_memory_documents(store, query).await?;
    let days = normalize_days(query.days);
    let end_date = query
        .end_date
        .unwrap_or_else(|| chrono::Utc::now().date_naive());
    let start_date = end_date - Duration::days(days.saturating_sub(1) as i64);
    let granularity = resolve_timeline_granularity(query.granularity.as_deref());

    let mut created_counts: BTreeMap<NaiveDate, u64> = BTreeMap::new();
    let mut updated_counts: BTreeMap<NaiveDate, u64> = BTreeMap::new();
    let mut prior_total = 0_u64;
    let mut periods = BTreeSet::new();

    for doc in &docs {
        if let Some(created_date) = parse_memory_date(&doc.frontmatter.created_at) {
            let bucket = period_start(created_date, granularity);
            if created_date < start_date {
                prior_total += 1;
            } else if created_date <= end_date {
                *created_counts.entry(bucket).or_insert(0) += 1;
                periods.insert(bucket);
            }
        }

        if let Some(updated_date) = parse_memory_date(&doc.frontmatter.updated_at) {
            if updated_date >= start_date && updated_date <= end_date {
                let bucket = period_start(updated_date, granularity);
                *updated_counts.entry(bucket).or_insert(0) += 1;
                periods.insert(bucket);
            }
        }
    }

    if periods.is_empty() {
        periods.insert(period_start(start_date, granularity));
    }

    let mut running_total = prior_total;
    let mut timeline = Vec::new();
    for period in periods {
        let period_end = match granularity {
            TimelineGranularity::Daily => period,
            TimelineGranularity::Weekly => (period + Duration::days(6)).min(end_date),
            TimelineGranularity::Monthly => {
                let next_month = if period.month() == 12 {
                    NaiveDate::from_ymd_opt(period.year() + 1, 1, 1).unwrap_or(period)
                } else {
                    NaiveDate::from_ymd_opt(period.year(), period.month() + 1, 1).unwrap_or(period)
                };
                (next_month - Duration::days(1)).min(end_date)
            }
        };
        let created_memories = created_counts.get(&period).copied().unwrap_or(0);
        let updated_memories = updated_counts.get(&period).copied().unwrap_or(0);
        running_total += created_memories;

        timeline.push(MemoryTimelinePoint {
            label: period_label(period, period_end),
            period_start: period.to_string(),
            period_end: period_end.to_string(),
            created_memories,
            updated_memories,
            total_memories: running_total,
        });
    }

    Ok(timeline)
}

/// Gets an aggregated durable memory summary for metrics dashboards.
///
/// # HTTP Route
/// `GET /metrics/memory/summary`
pub async fn memory_summary(
    state: web::Data<AppState>,
    query: web::Query<MemoryMetricsQuery>,
) -> impl Responder {
    if matches!(query.scope, Some(MemoryScope::Session)) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "session scope is not supported for memory metrics summary",
        }));
    }

    let store = MemoryStore::new(state.app_data_dir.clone());
    match build_memory_summary(
        &store,
        &state.session_store,
        state.storage.as_ref(),
        &query.into_inner(),
    )
    .await
    {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(error) => internal_error(error),
    }
}

/// Gets a durable memory activity and inventory timeline for charting.
///
/// # HTTP Route
/// `GET /metrics/memory/timeline`
pub async fn memory_timeline(
    state: web::Data<AppState>,
    query: web::Query<MemoryMetricsQuery>,
) -> impl Responder {
    if matches!(query.scope, Some(MemoryScope::Session)) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "session scope is not supported for memory metrics timeline",
        }));
    }

    let store = MemoryStore::new(state.app_data_dir.clone());
    match build_memory_timeline(&store, &query.into_inner()).await {
        Ok(timeline) => HttpResponse::Ok().json(timeline),
        Err(error) => internal_error(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use tempfile::tempdir;

    use bamboo_agent_core::storage::Storage;
    use bamboo_memory::memory_store::DurableMemoryType;
    use bamboo_storage::SessionStoreV2;

    async fn create_session_storage(
        dir: &std::path::Path,
    ) -> (Arc<SessionStoreV2>, Arc<dyn Storage>) {
        let session_store = Arc::new(
            SessionStoreV2::new(dir.to_path_buf())
                .await
                .expect("session store"),
        );
        let storage: Arc<dyn Storage> = session_store.clone();
        (session_store, storage)
    }

    #[tokio::test]
    async fn build_memory_summary_aggregates_global_and_project_scopes() {
        let dir = tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());
        let (session_store, storage) = create_session_storage(dir.path()).await;

        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Global reference",
                "Reference content",
                &[],
                Some("session-1"),
                "tester",
                false,
            )
            .await
            .expect("write global memory");
        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Project note",
                "Project memory content",
                &[],
                Some("session-1"),
                "tester",
                false,
            )
            .await
            .expect("write project memory");
        store
            .rebuild_scope(MemoryScope::Project, Some("proj-1"))
            .await
            .expect("rebuild scope");

        let summary = build_memory_summary(
            &store,
            &session_store,
            storage.as_ref(),
            &MemoryMetricsQuery {
                scope: None,
                project_key: None,
                days: None,
                end_date: None,
                granularity: None,
            },
        )
        .await
        .expect("aggregate summary");

        assert_eq!(summary.total_memories, 2);
        assert_eq!(summary.project_count, 1);
        assert_eq!(summary.by_scope.get("global"), Some(&1));
        assert_eq!(summary.by_scope.get("project"), Some(&1));
        assert_eq!(summary.by_type.get("reference"), Some(&1));
        assert_eq!(summary.by_type.get("project"), Some(&1));
        assert!(summary.last_reindex_at.is_some());
    }

    #[tokio::test]
    async fn build_memory_summary_aggregates_all_projects_for_project_scope() {
        let dir = tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());
        let (session_store, storage) = create_session_storage(dir.path()).await;

        for project_key in ["proj-a", "proj-b"] {
            store
                .write_memory(
                    MemoryScope::Project,
                    Some(project_key),
                    DurableMemoryType::Project,
                    &format!("Note for {project_key}"),
                    "Project-specific memory",
                    &[],
                    Some("session-1"),
                    "tester",
                    false,
                )
                .await
                .expect("write project memory");
        }

        let summary = build_memory_summary(
            &store,
            &session_store,
            storage.as_ref(),
            &MemoryMetricsQuery {
                scope: Some(MemoryScope::Project),
                project_key: None,
                days: None,
                end_date: None,
                granularity: None,
            },
        )
        .await
        .expect("project summary");

        assert_eq!(summary.scope, Some(MemoryScope::Project));
        assert_eq!(summary.total_memories, 2);
        assert_eq!(summary.project_count, 2);
        assert_eq!(summary.by_scope.get("project"), Some(&2));
    }

    #[tokio::test]
    async fn build_memory_summary_rejects_session_scope() {
        let dir = tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());
        let (session_store, storage) = create_session_storage(dir.path()).await;

        let error = build_memory_summary(
            &store,
            &session_store,
            storage.as_ref(),
            &MemoryMetricsQuery {
                scope: Some(MemoryScope::Session),
                project_key: None,
                days: None,
                end_date: None,
                granularity: None,
            },
        )
        .await
        .expect_err("session scope should be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("session scope is not supported"));
    }

    #[tokio::test]
    async fn build_memory_summary_includes_prompt_memory_observability_aggregates() {
        let dir = tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());
        let (session_store, storage) = create_session_storage(dir.path()).await;

        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Project note",
                "Project memory content",
                &[],
                Some("session-metrics-a"),
                "tester",
                false,
            )
            .await
            .expect("write project memory");

        let mut session_a = bamboo_agent_core::Session::new("session-metrics-a", "test-model");
        session_a.metadata.insert(
            "runtime_prompt_memory_observability".to_string(),
            serde_json::to_string(&PromptMemoryObservability {
                project_prompt_injection_enabled: true,
                relevant_recall_enabled: true,
                relevant_recall_rerank_enabled: true,
                project_first_dream_enabled: true,
                latest_user_query_present: true,
                resolved_project_key: Some("proj-1".to_string()),
                session_notes_status: "loaded".to_string(),
                project_memory_index_status: "loaded".to_string(),
                relevant_memory_status: "reranked".to_string(),
                project_dream_status: "loaded".to_string(),
                global_dream_fallback_status: "skipped_project_memory_or_dream_present".to_string(),
                dream_source: "project".to_string(),
                session_topic_count: 1,
                truncated_session_topic_count: 0,
                relevant_memory_count: 2,
                session_note_section_chars: 120,
                project_memory_index_section_chars: 300,
                relevant_memory_section_chars: 180,
                project_dream_section_chars: 240,
                global_dream_fallback_section_chars: 0,
                context_pressure_warning_chars: 0,
                external_memory_section_chars: 900,
            })
            .expect("serialize observability"),
        );
        storage
            .save_session(&session_a)
            .await
            .expect("save session a");

        let mut session_b = bamboo_agent_core::Session::new("session-metrics-b", "test-model");
        session_b.metadata.insert(
            "runtime_prompt_memory_observability".to_string(),
            serde_json::to_string(&PromptMemoryObservability {
                project_prompt_injection_enabled: true,
                relevant_recall_enabled: true,
                relevant_recall_rerank_enabled: true,
                project_first_dream_enabled: true,
                latest_user_query_present: true,
                resolved_project_key: None,
                session_notes_status: "empty".to_string(),
                project_memory_index_status: "no_project_key".to_string(),
                relevant_memory_status: "rerank_fallback".to_string(),
                project_dream_status: "no_project_key".to_string(),
                global_dream_fallback_status: "fallback_loaded".to_string(),
                dream_source: "global_fallback".to_string(),
                session_topic_count: 0,
                truncated_session_topic_count: 0,
                relevant_memory_count: 1,
                session_note_section_chars: 0,
                project_memory_index_section_chars: 0,
                relevant_memory_section_chars: 120,
                project_dream_section_chars: 0,
                global_dream_fallback_section_chars: 220,
                context_pressure_warning_chars: 64,
                external_memory_section_chars: 700,
            })
            .expect("serialize observability"),
        );
        storage
            .save_session(&session_b)
            .await
            .expect("save session b");

        let summary = build_memory_summary(
            &store,
            &session_store,
            storage.as_ref(),
            &MemoryMetricsQuery {
                scope: None,
                project_key: None,
                days: None,
                end_date: None,
                granularity: None,
            },
        )
        .await
        .expect("summary should succeed");

        let prompt_memory = summary
            .prompt_memory
            .expect("prompt memory summary should exist");
        assert_eq!(prompt_memory.observed_sessions, 2);
        assert_eq!(prompt_memory.project_memory_index_hits, 1);
        assert_eq!(prompt_memory.relevant_memory_hits, 2);
        assert_eq!(prompt_memory.relevant_memory_reranked_hits, 1);
        assert_eq!(prompt_memory.relevant_memory_rerank_enabled_sessions, 2);
        assert_eq!(prompt_memory.relevant_memory_rerank_fallbacks, 1);
        assert_eq!(prompt_memory.project_dream_hits, 1);
        assert_eq!(prompt_memory.global_dream_fallback_hits, 1);
        assert_eq!(prompt_memory.context_pressure_warning_hits, 1);
        assert_eq!(prompt_memory.total_relevant_memory_count, 3);
        assert_eq!(prompt_memory.avg_relevant_memory_count, 1);
        assert_eq!(prompt_memory.avg_relevant_memory_section_chars, 150);
        assert_eq!(prompt_memory.avg_external_memory_section_chars, 800);
        assert_eq!(
            prompt_memory
                .relevant_memory_status_breakdown
                .get("reranked"),
            Some(&1)
        );
        assert_eq!(
            prompt_memory
                .relevant_memory_status_breakdown
                .get("rerank_fallback"),
            Some(&1)
        );
        assert_eq!(
            prompt_memory.dream_source_breakdown.get("project"),
            Some(&1)
        );
        assert_eq!(
            prompt_memory.dream_source_breakdown.get("global_fallback"),
            Some(&1)
        );
        assert_eq!(
            prompt_memory.resolved_scope_breakdown.get("project"),
            Some(&1)
        );
        assert_eq!(
            prompt_memory
                .resolved_scope_breakdown
                .get("global_or_unscoped"),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn build_memory_timeline_tracks_created_and_running_total() {
        let dir = tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());

        let first = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Older note",
                "Created earlier",
                &[],
                Some("session-1"),
                "tester",
                false,
            )
            .await
            .expect("write first memory");
        let second = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Later note",
                "Created later",
                &[],
                Some("session-1"),
                "tester",
                false,
            )
            .await
            .expect("write second memory");

        let first_path = first.path.clone();
        let second_path = second.path.clone();
        let first_raw = tokio::fs::read_to_string(&first_path)
            .await
            .expect("read first");
        let second_raw = tokio::fs::read_to_string(&second_path)
            .await
            .expect("read second");
        tokio::fs::write(
            &first_path,
            first_raw
                .replace(&first.frontmatter.created_at, "2026-03-30T10:00:00+00:00")
                .replace(&first.frontmatter.updated_at, "2026-03-30T10:00:00+00:00"),
        )
        .await
        .expect("rewrite first timestamps");
        tokio::fs::write(
            &second_path,
            second_raw
                .replace(&second.frontmatter.created_at, "2026-04-01T12:00:00+00:00")
                .replace(&second.frontmatter.updated_at, "2026-04-02T09:30:00+00:00"),
        )
        .await
        .expect("rewrite second timestamps");

        let timeline = build_memory_timeline(
            &store,
            &MemoryMetricsQuery {
                scope: None,
                project_key: None,
                days: Some(4),
                end_date: Some(NaiveDate::from_ymd_opt(2026, 4, 2).expect("date")),
                granularity: Some("daily".to_string()),
            },
        )
        .await
        .expect("timeline");

        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].period_start, "2026-03-30");
        assert_eq!(timeline[0].created_memories, 1);
        assert_eq!(timeline[0].total_memories, 1);
        assert_eq!(timeline[1].period_start, "2026-04-01");
        assert_eq!(timeline[1].created_memories, 1);
        assert_eq!(timeline[1].updated_memories, 1);
        assert_eq!(timeline[1].total_memories, 2);
    }

    #[test]
    fn summarize_memory_results_prefers_latest_timestamps_and_accumulates_breakdowns() {
        let result_a = MemoryInspectResult {
            scope: MemoryScope::Global,
            project_key: None,
            total_memories: 2,
            by_type: BTreeMap::from([("project".to_string(), 1), ("reference".to_string(), 1)]),
            by_status: BTreeMap::from([("active".to_string(), 2)]),
            recent_ids: vec![],
            view_files: vec![],
            index_files: vec![],
            state_files: vec![],
            stale_candidate_count: 1,
            last_reindex_at: Some("2026-04-05T02:00:00Z".to_string()),
            last_dream_at: Some("2026-04-05T02:05:00Z".to_string()),
            topic_paths: vec![],
        };
        let result_b = MemoryInspectResult {
            scope: MemoryScope::Project,
            project_key: Some("proj-1".to_string()),
            total_memories: 3,
            by_type: BTreeMap::from([("project".to_string(), 3)]),
            by_status: BTreeMap::from([("active".to_string(), 2), ("stale".to_string(), 1)]),
            recent_ids: vec![],
            view_files: vec![],
            index_files: vec![],
            state_files: vec![],
            stale_candidate_count: 2,
            last_reindex_at: Some("2026-04-05T03:00:00Z".to_string()),
            last_dream_at: Some("2026-04-05T01:00:00Z".to_string()),
            topic_paths: vec![],
        };

        let summary = summarize_memory_results(&[result_a, result_b], None, None, 1, None);
        assert_eq!(summary.total_memories, 5);
        assert_eq!(summary.stale_candidate_count, 3);
        assert_eq!(summary.by_type.get("project"), Some(&4));
        assert_eq!(summary.by_type.get("reference"), Some(&1));
        assert_eq!(summary.by_status.get("active"), Some(&4));
        assert_eq!(summary.by_status.get("stale"), Some(&1));
        assert_eq!(summary.by_scope.get("global"), Some(&2));
        assert_eq!(summary.by_scope.get("project"), Some(&3));
        assert_eq!(
            summary.last_reindex_at.as_deref(),
            Some("2026-04-05T03:00:00+00:00")
        );
        assert_eq!(
            summary.last_dream_at.as_deref(),
            Some("2026-04-05T02:05:00+00:00")
        );
    }
}
