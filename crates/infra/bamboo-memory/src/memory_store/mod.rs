//! Bamboo's narrow native facade over the Jiandu memory store.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use jiandu_memory::memory_store::{
    count_chars, normalize_retrieval_terms, normalize_tags, render_memory_freshness_note,
    summary_json, truncate_chars, BlobScanItem, BlobScanReport, DreamReadResult, DreamSnapshot,
    DuplicateCluster, DuplicateScanReport, DurableMemoryDocument, DurableMemoryStatus,
    DurableMemoryType, FreshnessKind, MemoryConsolidateResult, MemoryContradictionResult,
    MemoryDuplicateCandidate, MemoryMergeResult, MemoryPurgeResult, MemoryQueryOptions,
    MemoryQueryResult, MemoryRecallCandidate, MemoryRecallOptions, MemoryRetrievalInput,
    MemoryScope, MemorySplitPiece, MemorySplitResult, SessionState, TemporalGranularity,
    DEFAULT_QUERY_LIMIT, DEFAULT_SESSION_TOPIC, MAX_EXPLICIT_MEMORY_ENTITIES,
    MAX_EXPLICIT_MEMORY_KEYWORDS, MAX_MAX_CHARS, MAX_MEMORY_ENTITIES, MAX_MEMORY_ID_LEN,
    MAX_MEMORY_KEYWORDS, MAX_MEMORY_QUERY_CHARS, MAX_MEMORY_TAGS, MAX_MEMORY_TAG_CHARS,
    MAX_MEMORY_TITLE_LEN, MAX_QUERY_LIMIT, MAX_RETRIEVAL_TERM_CHARS,
};

/// Jiandu's inspection result plus Bamboo's flattened Dream timestamp.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryInspectResult {
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    pub total_memories: usize,
    #[serde(default)]
    pub by_type: BTreeMap<String, usize>,
    #[serde(default)]
    pub by_status: BTreeMap<String, usize>,
    #[serde(default)]
    pub recent_ids: Vec<String>,
    #[serde(default)]
    pub view_files: Vec<String>,
    #[serde(default)]
    pub index_files: Vec<String>,
    #[serde(default)]
    pub state_files: Vec<String>,
    #[serde(default)]
    pub stale_candidate_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reindex_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dream_at: Option<String>,
    #[serde(default)]
    pub topic_paths: Vec<String>,
}

/// Bamboo-facing memory handle. Jiandu remains private so Bamboo callers cannot
/// bypass the facade's Project identity and inspection extensions.
#[derive(Debug, Clone)]
pub struct MemoryStore {
    store: jiandu_memory::memory_store::MemoryStore,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl MemoryStore {
    /// Construct a Jiandu store rooted at an explicit data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            store: jiandu_memory::memory_store::MemoryStore::new(data_dir),
        }
    }

    /// Construct the production store at the independent `~/.jiandu` root.
    pub fn with_defaults() -> Self {
        Self::new(default_jiandu_data_dir())
    }

    /// Bind Project memory to Bamboo's first-class Project identity.
    pub fn for_project(&self, project_id: &bamboo_domain::ProjectId) -> Self {
        let project_id = jiandu_memory::ProjectId::parse(project_id.as_str().to_owned())
            .expect("Bamboo ProjectId must satisfy Jiandu's identical path-safe contract");
        Self {
            store: self.store.for_project(&project_id),
        }
    }

    pub async fn read_session_topic(
        &self,
        session_id: &str,
        topic: &str,
    ) -> io::Result<Option<String>> {
        self.store.read_session_topic(session_id, topic).await
    }

    pub async fn write_session_topic(
        &self,
        session_id: &str,
        topic: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
        self.store
            .write_session_topic(session_id, topic, content)
            .await
    }

    pub async fn delete_session_topic(&self, session_id: &str, topic: &str) -> io::Result<bool> {
        self.store.delete_session_topic(session_id, topic).await
    }

    pub async fn list_session_topics(&self, session_id: &str) -> io::Result<Vec<String>> {
        self.store.list_session_topics(session_id).await
    }

    pub async fn read_session_topics_with_content(
        &self,
        session_id: &str,
    ) -> io::Result<Vec<(String, String)>> {
        self.store
            .read_session_topics_with_content(session_id)
            .await
    }

    pub async fn read_session_state(&self, session_id: &str) -> io::Result<SessionState> {
        self.store.read_session_state(session_id).await
    }

    pub async fn mark_session_extracted(
        &self,
        session_id: &str,
        extracted_at: &str,
    ) -> io::Result<()> {
        self.store
            .mark_session_extracted(session_id, extracted_at)
            .await
    }

    pub async fn read_memory_view(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<String>> {
        self.store.read_memory_view(scope, project_key).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn query_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        query: Option<&str>,
        filter_types: Option<&HashSet<DurableMemoryType>>,
        filter_statuses: Option<&HashSet<DurableMemoryStatus>>,
        filter_granularity: Option<&HashSet<TemporalGranularity>>,
        options: &MemoryQueryOptions,
    ) -> io::Result<MemoryQueryResult> {
        self.store
            .query_scope(
                scope,
                project_key,
                query,
                filter_types,
                filter_statuses,
                filter_granularity,
                options,
            )
            .await
    }

    pub async fn inspect_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<MemoryInspectResult> {
        let result = self.store.inspect_scope(scope, project_key).await?;
        let last_dream_at = if scope == MemoryScope::Session {
            None
        } else {
            self.store
                .read_dream_snapshot(scope, project_key)
                .await?
                .snapshot
                .map(|snapshot| snapshot.generated_at)
        };

        Ok(MemoryInspectResult {
            scope: result.scope,
            project_key: result.project_key,
            total_memories: result.total_memories,
            by_type: result.by_type,
            by_status: result.by_status,
            recent_ids: result.recent_ids,
            view_files: result.view_files,
            index_files: result.index_files,
            state_files: result.state_files,
            stale_candidate_count: result.stale_candidate_count,
            last_reindex_at: result.last_reindex_at,
            last_dream_at,
            topic_paths: result.topic_paths,
        })
    }

    pub async fn get_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
    ) -> io::Result<Option<DurableMemoryDocument>> {
        self.store.get_memory(id, preferred_project_key).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_memory(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        r#type: DurableMemoryType,
        title: &str,
        content: &str,
        tags: &[String],
        session_id: Option<&str>,
        actor: &str,
        allow_merge_if_similar: bool,
        granularity: Option<TemporalGranularity>,
    ) -> io::Result<DurableMemoryDocument> {
        self.store
            .write_memory(
                scope,
                project_key,
                r#type,
                title,
                content,
                tags,
                session_id,
                actor,
                allow_merge_if_similar,
                granularity,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_memory_with_retrieval(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        r#type: DurableMemoryType,
        title: &str,
        content: &str,
        tags: &[String],
        retrieval: &MemoryRetrievalInput,
        session_id: Option<&str>,
        actor: &str,
        allow_merge_if_similar: bool,
        granularity: Option<TemporalGranularity>,
    ) -> io::Result<DurableMemoryDocument> {
        self.store
            .write_memory_with_retrieval(
                scope,
                project_key,
                r#type,
                title,
                content,
                tags,
                retrieval,
                session_id,
                actor,
                allow_merge_if_similar,
                granularity,
            )
            .await
    }

    pub async fn archive_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        mode: DurableMemoryStatus,
        reason: Option<&str>,
    ) -> io::Result<Option<DurableMemoryDocument>> {
        self.store
            .archive_memory(id, preferred_project_key, mode, reason)
            .await
    }

    pub async fn split_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        pieces: &[MemorySplitPiece],
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemorySplitResult>> {
        self.store
            .split_memory(id, preferred_project_key, pieces, session_id, actor)
            .await
    }

    pub async fn split_memory_with_retrieval(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        pieces: &[MemorySplitPiece],
        retrieval: &[MemoryRetrievalInput],
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemorySplitResult>> {
        self.store
            .split_memory_with_retrieval(
                id,
                preferred_project_key,
                pieces,
                retrieval,
                session_id,
                actor,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn find_duplicate_candidates(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        r#type: Option<DurableMemoryType>,
        title: &str,
        content: &str,
        tags: &[String],
        limit: usize,
    ) -> io::Result<Vec<MemoryDuplicateCandidate>> {
        self.store
            .find_duplicate_candidates(scope, project_key, r#type, title, content, tags, limit)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn find_duplicate_candidates_with_retrieval(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        r#type: Option<DurableMemoryType>,
        title: &str,
        content: &str,
        tags: &[String],
        retrieval: &MemoryRetrievalInput,
        limit: usize,
    ) -> io::Result<Vec<MemoryDuplicateCandidate>> {
        self.store
            .find_duplicate_candidates_with_retrieval(
                scope,
                project_key,
                r#type,
                title,
                content,
                tags,
                retrieval,
                limit,
            )
            .await
    }

    pub async fn scan_blob_candidates(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        min_appended_sections: usize,
        limit: usize,
    ) -> io::Result<BlobScanReport> {
        self.store
            .scan_blob_candidates(scope, project_key, min_appended_sections, limit)
            .await
    }

    pub async fn scan_duplicate_clusters(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        min_score: f64,
        max_members_per_cluster: usize,
        limit: usize,
    ) -> io::Result<DuplicateScanReport> {
        self.store
            .scan_duplicate_clusters(
                scope,
                project_key,
                min_score,
                max_members_per_cluster,
                limit,
            )
            .await
    }

    pub async fn consolidate_memories(
        &self,
        ids: &[String],
        preferred_project_key: Option<&str>,
        merged: &MemorySplitPiece,
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemoryConsolidateResult>> {
        self.store
            .consolidate_memories(ids, preferred_project_key, merged, session_id, actor)
            .await
    }

    pub async fn consolidate_memories_with_retrieval(
        &self,
        ids: &[String],
        preferred_project_key: Option<&str>,
        merged: &MemorySplitPiece,
        retrieval: &MemoryRetrievalInput,
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemoryConsolidateResult>> {
        self.store
            .consolidate_memories_with_retrieval(
                ids,
                preferred_project_key,
                merged,
                retrieval,
                session_id,
                actor,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn purge_memories(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        filter_types: Option<&HashSet<DurableMemoryType>>,
        filter_statuses: Option<&HashSet<DurableMemoryStatus>>,
        filter_granularity: Option<&HashSet<TemporalGranularity>>,
        mode: DurableMemoryStatus,
        reason: Option<&str>,
    ) -> io::Result<MemoryPurgeResult> {
        self.store
            .purge_memories(
                scope,
                project_key,
                filter_types,
                filter_statuses,
                filter_granularity,
                mode,
                reason,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn mark_memory_contradicted(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        contradicted_by_ids: &[String],
        reason: Option<&str>,
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemoryContradictionResult>> {
        self.store
            .mark_memory_contradicted(
                id,
                preferred_project_key,
                contradicted_by_ids,
                reason,
                session_id,
                actor,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn merge_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        content: &str,
        tags: &[String],
        session_id: Option<&str>,
        actor: &str,
        source_memory_ids: &[String],
    ) -> io::Result<Option<MemoryMergeResult>> {
        self.store
            .merge_memory(
                id,
                preferred_project_key,
                content,
                tags,
                session_id,
                actor,
                source_memory_ids,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn merge_memory_with_retrieval(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        content: &str,
        tags: &[String],
        retrieval: &MemoryRetrievalInput,
        session_id: Option<&str>,
        actor: &str,
        source_memory_ids: &[String],
    ) -> io::Result<Option<MemoryMergeResult>> {
        self.store
            .merge_memory_with_retrieval(
                id,
                preferred_project_key,
                content,
                tags,
                retrieval,
                session_id,
                actor,
                source_memory_ids,
            )
            .await
    }

    pub async fn rebuild_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<()> {
        self.store.rebuild_scope(scope, project_key).await
    }

    pub async fn current_scope_generation(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<String> {
        self.store
            .current_scope_generation(scope, project_key)
            .await
    }

    pub async fn read_dream_snapshot(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<DreamReadResult> {
        self.store.read_dream_snapshot(scope, project_key).await
    }

    pub async fn publish_dream_snapshot(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        source_generation: &str,
        content: &str,
    ) -> io::Result<DreamSnapshot> {
        self.store
            .publish_dream_snapshot(scope, project_key, source_generation, content)
            .await
    }

    pub async fn list_memory_documents(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Vec<DurableMemoryDocument>> {
        self.store.list_memory_documents(scope, project_key).await
    }

    pub async fn count_scope_memories(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<usize> {
        self.store.count_scope_memories(scope, project_key).await
    }

    pub async fn enforce_scope_capacity(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        capacity: usize,
        max_archivals: usize,
    ) -> io::Result<Vec<String>> {
        self.store
            .enforce_scope_capacity(scope, project_key, capacity, max_archivals)
            .await
    }

    pub async fn expire_stale_granularity(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Vec<String>> {
        self.store
            .expire_stale_granularity(scope, project_key)
            .await
    }
}

/// Run Jiandu's deterministic lexical shortlist without exposing the composed
/// store to Bamboo callers.
pub async fn shortlist_relevant_memories(
    store: &MemoryStore,
    project_key: Option<&str>,
    query: &str,
    options: &MemoryRecallOptions,
) -> io::Result<Vec<MemoryRecallCandidate>> {
    jiandu_memory::memory_store::shortlist_relevant_memories(
        &store.store,
        project_key,
        query,
        options,
    )
    .await
}

fn default_jiandu_data_dir() -> PathBuf {
    dirs::home_dir().map_or_else(|| PathBuf::from(".jiandu"), |home| home.join(".jiandu"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn write_durable(
        store: &MemoryStore,
        scope: MemoryScope,
        project_key: Option<&str>,
        title: &str,
        body: &str,
    ) -> DurableMemoryDocument {
        store
            .write_memory(
                scope,
                project_key,
                if scope == MemoryScope::Project {
                    DurableMemoryType::Project
                } else {
                    DurableMemoryType::Reference
                },
                title,
                body,
                &["facade-test".to_string()],
                Some("session-test"),
                "facade-test",
                false,
                None,
            )
            .await
            .expect("write durable memory")
    }

    #[test]
    fn default_root_is_dot_jiandu_under_home() {
        let expected =
            dirs::home_dir().map_or_else(|| PathBuf::from(".jiandu"), |home| home.join(".jiandu"));
        assert_eq!(default_jiandu_data_dir(), expected);
    }

    #[tokio::test]
    async fn session_global_and_typed_project_round_trip_with_scope_isolation() {
        let directory = tempdir().expect("tempdir");
        let store = MemoryStore::new(directory.path());

        store
            .write_session_topic("session_1", DEFAULT_SESSION_TOPIC, "session note")
            .await
            .expect("write session note");
        assert_eq!(
            store
                .read_session_topic("session_1", DEFAULT_SESSION_TOPIC)
                .await
                .expect("read session note")
                .as_deref(),
            Some("session note")
        );

        let global = write_durable(
            &store,
            MemoryScope::Global,
            None,
            "Global decision",
            "Prefer deterministic lexical memory.",
        )
        .await;

        let project_id =
            bamboo_domain::ProjectId::parse("project_alpha").expect("valid Bamboo ProjectId");
        let project_store = store.for_project(&project_id);
        let project = write_durable(
            &project_store,
            MemoryScope::Project,
            Some(project_id.as_str()),
            "Project decision",
            "Project alpha uses the Jiandu facade.",
        )
        .await;

        assert!(project
            .path
            .starts_with(directory.path().join("projects/project_alpha/memory/v1")));
        assert_eq!(
            store
                .get_memory(&global.frontmatter.id, None)
                .await
                .expect("read global")
                .expect("global exists")
                .body,
            global.body
        );
        assert!(
            store
                .get_memory(&project.frontmatter.id, None)
                .await
                .expect("unscoped lookup")
                .is_none(),
            "unscoped lookup must not scan Projects"
        );
        assert!(project_store
            .get_memory(&project.frontmatter.id, Some(project_id.as_str()))
            .await
            .expect("typed Project lookup")
            .is_some());

        let other_id =
            bamboo_domain::ProjectId::parse("project_beta").expect("valid Bamboo ProjectId");
        assert!(store
            .for_project(&other_id)
            .get_memory(&project.frontmatter.id, Some(other_id.as_str()))
            .await
            .expect("unrelated Project lookup")
            .is_none());
    }

    #[tokio::test]
    async fn inspect_reads_dream_timestamp_from_the_same_scope() {
        let directory = tempdir().expect("tempdir");
        let store = MemoryStore::new(directory.path());
        write_durable(
            &store,
            MemoryScope::Global,
            None,
            "Global fact",
            "Global memory remains separate.",
        )
        .await;

        let global_generation = store
            .current_scope_generation(MemoryScope::Global, None)
            .await
            .expect("global generation");
        let global_dream = store
            .publish_dream_snapshot(
                MemoryScope::Global,
                None,
                &global_generation,
                "Global orientation",
            )
            .await
            .expect("publish global Dream");

        let project_id =
            bamboo_domain::ProjectId::parse("project_dream").expect("valid Bamboo ProjectId");
        let project_store = store.for_project(&project_id);
        write_durable(
            &project_store,
            MemoryScope::Project,
            Some(project_id.as_str()),
            "Project fact",
            "Project memory remains separate.",
        )
        .await;

        let before_project_dream = project_store
            .inspect_scope(MemoryScope::Project, Some(project_id.as_str()))
            .await
            .expect("inspect Project before Dream");
        assert_eq!(before_project_dream.last_dream_at, None);

        let project_generation = project_store
            .current_scope_generation(MemoryScope::Project, Some(project_id.as_str()))
            .await
            .expect("Project generation");
        let project_dream = project_store
            .publish_dream_snapshot(
                MemoryScope::Project,
                Some(project_id.as_str()),
                &project_generation,
                "Project orientation",
            )
            .await
            .expect("publish Project Dream");

        assert_eq!(
            store
                .inspect_scope(MemoryScope::Global, None)
                .await
                .expect("inspect Global")
                .last_dream_at,
            Some(global_dream.generated_at)
        );
        assert_eq!(
            project_store
                .inspect_scope(MemoryScope::Project, Some(project_id.as_str()))
                .await
                .expect("inspect Project")
                .last_dream_at,
            Some(project_dream.generated_at)
        );
    }

    #[tokio::test]
    async fn dream_cold_success_stale_and_failed_cas_preserve_prior_snapshot() {
        let directory = tempdir().expect("tempdir");
        let store = MemoryStore::new(directory.path());

        let cold = store
            .read_dream_snapshot(MemoryScope::Global, None)
            .await
            .expect("read cold Dream");
        assert!(cold.snapshot.is_none());
        assert!(!cold.stale);

        let initial = store
            .publish_dream_snapshot(
                MemoryScope::Global,
                None,
                &cold.current_generation,
                "Initial complete orientation",
            )
            .await
            .expect("publish initial Dream");
        let fresh = store
            .read_dream_snapshot(MemoryScope::Global, None)
            .await
            .expect("read fresh Dream");
        assert_eq!(fresh.snapshot.as_ref(), Some(&initial));
        assert!(!fresh.stale);

        write_durable(
            &store,
            MemoryScope::Global,
            None,
            "Concurrent fact",
            "Canonical memory changed after synthesis began.",
        )
        .await;
        let stale = store
            .read_dream_snapshot(MemoryScope::Global, None)
            .await
            .expect("read stale Dream");
        assert!(stale.stale);
        assert_eq!(stale.snapshot.as_ref(), Some(&initial));

        let error = store
            .publish_dream_snapshot(
                MemoryScope::Global,
                None,
                &cold.current_generation,
                "Must not replace the complete snapshot",
            )
            .await
            .expect_err("stale generation must fail CAS");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            store
                .read_dream_snapshot(MemoryScope::Global, None)
                .await
                .expect("read preserved Dream")
                .snapshot,
            Some(initial)
        );
    }

    #[tokio::test]
    async fn rebuild_preserves_a_complete_dream_snapshot() {
        let directory = tempdir().expect("tempdir");
        let store = MemoryStore::new(directory.path());
        write_durable(
            &store,
            MemoryScope::Global,
            None,
            "Rebuild fact",
            "Derived indexes can rebuild without replacing Dream.",
        )
        .await;
        let generation = store
            .current_scope_generation(MemoryScope::Global, None)
            .await
            .expect("generation");
        let dream = store
            .publish_dream_snapshot(
                MemoryScope::Global,
                None,
                &generation,
                "Stable complete orientation",
            )
            .await
            .expect("publish Dream");

        store
            .rebuild_scope(MemoryScope::Global, None)
            .await
            .expect("rebuild scope");
        let after = store
            .read_dream_snapshot(MemoryScope::Global, None)
            .await
            .expect("read Dream after rebuild");
        assert!(!after.stale);
        assert_eq!(after.snapshot, Some(dream));
    }
}
