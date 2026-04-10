use std::cmp::Ordering;
use std::io;

use super::{
    extract_keywords, parse_rfc3339, DurableMemoryStatus, LexicalIndexItem, MemoryScope,
    MemoryStore,
};

#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRecallCandidate {
    pub id: String,
    pub title: String,
    pub score: f64,
    pub scope: MemoryScope,
    pub project_key: Option<String>,
    pub status: DurableMemoryStatus,
    pub updated_at: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRecallOptions {
    pub shortlist_limit: usize,
    pub include_global_fallback: bool,
    pub max_candidates_per_scope: usize,
}

impl Default for MemoryRecallOptions {
    fn default() -> Self {
        Self {
            shortlist_limit: 3,
            include_global_fallback: true,
            max_candidates_per_scope: 20,
        }
    }
}

pub async fn shortlist_relevant_memories(
    store: &MemoryStore,
    project_key: Option<&str>,
    query: &str,
    options: &MemoryRecallOptions,
) -> io::Result<Vec<MemoryRecallCandidate>> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let limit = options.shortlist_limit.max(1);
    let per_scope_limit = options.max_candidates_per_scope.max(limit);

    if let Some(project_key) = project_key.map(str::trim).filter(|value| !value.is_empty()) {
        let mut project_hits =
            shortlist_scope(store, MemoryScope::Project, Some(project_key), query).await?;
        project_hits.truncate(per_scope_limit);
        if !project_hits.is_empty() {
            project_hits.truncate(limit);
            return Ok(project_hits);
        }
    }

    if options.include_global_fallback {
        let mut global_hits = shortlist_scope(store, MemoryScope::Global, None, query).await?;
        global_hits.truncate(per_scope_limit);
        global_hits.truncate(limit);
        return Ok(global_hits);
    }

    Ok(Vec::new())
}

async fn shortlist_scope(
    store: &MemoryStore,
    scope: MemoryScope,
    project_key: Option<&str>,
    query: &str,
) -> io::Result<Vec<MemoryRecallCandidate>> {
    let Some(index) = store.read_lexical_index(scope, project_key).await? else {
        return Ok(Vec::new());
    };

    let query_tokens = extract_keywords(query, "", &[]);
    if query_tokens.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates = index
        .items
        .iter()
        .filter_map(|item| score_lexical_index_item(item, &query_tokens).map(|score| (item, score)))
        .map(|(item, score)| MemoryRecallCandidate {
            id: item.id.clone(),
            title: item.title.clone(),
            score,
            scope: item.scope,
            project_key: item.project_key.clone(),
            status: item.status,
            updated_at: item.updated_at.clone(),
            summary: item.summary.clone(),
        })
        .collect::<Vec<_>>();

    sort_recall_candidates(&mut candidates);
    Ok(candidates)
}

fn score_lexical_index_item(item: &LexicalIndexItem, query_tokens: &[String]) -> Option<f64> {
    match item.status {
        DurableMemoryStatus::Superseded
        | DurableMemoryStatus::Contradicted
        | DurableMemoryStatus::Archived => return None,
        DurableMemoryStatus::Active | DurableMemoryStatus::Stale => {}
    }

    let title = item.title.to_ascii_lowercase();
    let summary = item.summary.to_ascii_lowercase();

    let mut score = 0.0;
    let mut matched_any = false;

    for token in query_tokens {
        let mut token_score = 0.0;
        if title.contains(token) {
            token_score += 3.0;
        }
        if item
            .keywords
            .iter()
            .any(|value| value.eq_ignore_ascii_case(token))
        {
            token_score += 2.5;
        }
        if item
            .tags
            .iter()
            .any(|value| value.eq_ignore_ascii_case(token))
        {
            token_score += 2.0;
        }
        if item
            .entities
            .iter()
            .any(|value| value.eq_ignore_ascii_case(token))
        {
            token_score += 1.5;
        }
        if summary.contains(token) {
            token_score += 1.0;
        }
        if token_score > 0.0 {
            matched_any = true;
            score += token_score;
        }
    }

    if !matched_any {
        return None;
    }

    score += lexical_status_adjustment(item.status);
    Some((score / query_tokens.len() as f64 * 100.0).round() / 100.0)
}

fn lexical_status_adjustment(status: DurableMemoryStatus) -> f64 {
    match status {
        DurableMemoryStatus::Active => 0.0,
        DurableMemoryStatus::Stale => -0.75,
        DurableMemoryStatus::Superseded
        | DurableMemoryStatus::Contradicted
        | DurableMemoryStatus::Archived => -10.0,
    }
}

fn sort_recall_candidates(candidates: &mut [MemoryRecallCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                let left_dt = parse_rfc3339(&left.updated_at)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
                let right_dt = parse_rfc3339(&right.updated_at)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
                right_dt.cmp(&left_dt)
            })
            .then_with(|| left.title.cmp(&right.title))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::memory_store::DurableMemoryType;
    use tempfile::tempdir;

    fn item(
        id: &str,
        title: &str,
        status: DurableMemoryStatus,
        updated_at: &str,
        keywords: &[&str],
        tags: &[&str],
        entities: &[&str],
        summary: &str,
    ) -> LexicalIndexItem {
        LexicalIndexItem {
            id: id.to_string(),
            title: title.to_string(),
            scope: MemoryScope::Project,
            project_key: Some("proj-1".to_string()),
            r#type: DurableMemoryType::Project,
            status,
            tags: tags.iter().map(|v| v.to_string()).collect(),
            keywords: keywords.iter().map(|v| v.to_string()).collect(),
            entities: entities.iter().map(|v| v.to_string()).collect(),
            updated_at: updated_at.to_string(),
            created_at: updated_at.to_string(),
            summary: summary.to_string(),
        }
    }

    #[test]
    fn title_matches_outrank_keyword_only_matches() {
        let query_tokens = vec!["release".to_string(), "freeze".to_string()];
        let title_item = item(
            "a",
            "Release freeze decision",
            DurableMemoryStatus::Active,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );
        let keyword_item = item(
            "b",
            "Deployment decision",
            DurableMemoryStatus::Active,
            "2026-04-09T00:00:00Z",
            &["release", "freeze"],
            &[],
            &[],
            "summary",
        );

        let title_score = score_lexical_index_item(&title_item, &query_tokens).unwrap();
        let keyword_score = score_lexical_index_item(&keyword_item, &query_tokens).unwrap();
        assert!(title_score > keyword_score);
    }

    #[test]
    fn active_items_outrank_stale_items() {
        let query_tokens = vec!["release".to_string()];
        let active = item(
            "a",
            "Release freeze decision",
            DurableMemoryStatus::Active,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );
        let stale = item(
            "b",
            "Release freeze decision",
            DurableMemoryStatus::Stale,
            "2026-04-10T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );

        let active_score = score_lexical_index_item(&active, &query_tokens).unwrap();
        let stale_score = score_lexical_index_item(&stale, &query_tokens).unwrap();
        assert!(active_score > stale_score);
    }

    #[test]
    fn contradicted_and_archived_items_are_filtered_out() {
        let query_tokens = vec!["release".to_string()];
        let contradicted = item(
            "a",
            "Release freeze decision",
            DurableMemoryStatus::Contradicted,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );
        let archived = item(
            "b",
            "Release freeze decision",
            DurableMemoryStatus::Archived,
            "2026-04-09T00:00:00Z",
            &[],
            &[],
            &[],
            "summary",
        );

        assert!(score_lexical_index_item(&contradicted, &query_tokens).is_none());
        assert!(score_lexical_index_item(&archived, &query_tokens).is_none());
    }

    #[tokio::test]
    async fn project_scope_shortlist_excludes_global_when_project_hits_exist() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze decision",
                "Project-specific release freeze note.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Global release guidance",
                "Global note that should not be used when project hits exist.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();

        let candidates = shortlist_relevant_memories(
            &store,
            Some("proj-1"),
            "release freeze",
            &MemoryRecallOptions::default(),
        )
        .await
        .unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.scope == MemoryScope::Project));
    }

    #[tokio::test]
    async fn global_fallback_triggers_only_when_project_hits_are_absent() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Global release guidance",
                "Fallback note for release work.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
            )
            .await
            .unwrap();

        let candidates = shortlist_relevant_memories(
            &store,
            Some("proj-missing"),
            "release guidance",
            &MemoryRecallOptions::default(),
        )
        .await
        .unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.scope == MemoryScope::Global));
    }
}
