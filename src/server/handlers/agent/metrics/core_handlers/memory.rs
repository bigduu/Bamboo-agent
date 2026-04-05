use std::collections::{BTreeMap, BTreeSet};
use std::io;

use actix_web::{web, HttpResponse, Responder};
use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, Weekday};

use super::super::{internal_error, MemoryMetricsQuery, MemoryMetricsSummary, MemoryTimelinePoint};
use crate::agent::core::memory_store::{
    DurableMemoryDocument, MemoryInspectResult, MemoryScope, MemoryStore,
};
use crate::server::app_state::AppState;
use crate::server::handlers::agent::metrics::core_handlers::filters::{
    normalize_days, resolve_timeline_granularity, TimelineGranularity,
};

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

fn summarize_memory_results(
    results: &[MemoryInspectResult],
    scope: Option<MemoryScope>,
    project_key: Option<String>,
    project_count: u64,
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
    }
}

pub(crate) async fn build_memory_summary(
    store: &MemoryStore,
    query: &MemoryMetricsQuery,
) -> io::Result<MemoryMetricsSummary> {
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
    match build_memory_summary(&store, &query.into_inner()).await {
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
    use tempfile::tempdir;

    use crate::agent::core::memory_store::DurableMemoryType;

    #[tokio::test]
    async fn build_memory_summary_aggregates_global_and_project_scopes() {
        let dir = tempdir().expect("temp dir");
        let store = MemoryStore::new(dir.path());

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

        let error = build_memory_summary(
            &store,
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

        let summary = summarize_memory_results(&[result_a, result_b], None, None, 1);
        assert_eq!(summary.total_memories, 5);
        assert_eq!(summary.stale_candidate_count, 3);
        assert_eq!(summary.by_type.get("project"), Some(&4));
        assert_eq!(summary.by_type.get("reference"), Some(&1));
        assert_eq!(summary.by_status.get("active"), Some(&4));
        assert_eq!(summary.by_status.get("stale"), Some(&1));
        assert_eq!(summary.by_scope.get("global"), Some(&2));
        assert_eq!(summary.by_scope.get("project"), Some(&3));
        assert_eq!(summary.last_reindex_at.as_deref(), Some("2026-04-05T03:00:00+00:00"));
        assert_eq!(summary.last_dream_at.as_deref(), Some("2026-04-05T02:05:00+00:00"));
    }
}
