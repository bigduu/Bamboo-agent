use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use tokio::fs;
use tokio::sync::Mutex;

use bamboo_agent_core::workspace_state;

use super::access_log::{
    self, AccessLogEntry, AccessStats, ACCESS_LOG_COMPACT_TRIGGER_BYTES, ACCESS_LOG_FILE,
};
use super::freshness;
use super::lexical_bm25;
use super::{
    build_dream_view, build_memory_markdown_view, build_recent_markdown_view,
    build_stale_markdown_view, derive_summary, detect_entities, extract_keywords,
    make_query_cursor, match_memory_query, normalize_tags, now_rfc3339, parse_markdown_document,
    parse_query_cursor, parse_rfc3339, project_key_from_path, render_markdown_document,
    short_stable_hash, sort_memories_desc, validate_memory_title, validate_session_id,
    validate_session_topic, AuditLogEntry, GraphIndex, GraphIndexItem, LexicalIndex,
    LexicalIndexItem, MemoryContradictionResult, MemoryDuplicateCandidate, MemoryInspectResult,
    MemoryMergeResult, MemoryPathResolver, MemoryPurgeResult, MemoryQueryItem, MemoryQueryOptions,
    MemoryQueryResult, MemorySplitPiece, MemorySplitResult, RecentIndex, RecentIndexItem,
    StaleCandidateItem, StaleCandidatesIndex, TaxonomyIndex, CONTRADICTION_AUDIT_LOG,
    DEFAULT_MAX_CHARS, DEFAULT_QUERY_LIMIT, DEFAULT_SESSION_TOPIC, DREAM_VIEW_FILE,
    GRAPH_INDEX_FILE, LEXICAL_INDEX_FILE, MAX_MAX_CHARS, MAX_QUERY_LIMIT, MEMORY_SCHEMA_VERSION,
    MEMORY_VIEW_FILE, MERGE_AUDIT_LOG, PURGE_AUDIT_LOG, RECENT_INDEX_FILE, RECENT_VIEW_FILE,
    STALE_CANDIDATES_INDEX_FILE, STALE_VIEW_FILE, TAXONOMY_INDEX_FILE, WRITE_AUDIT_LOG,
};
use super::{
    BlobScanItem, BlobScanReport, DuplicateCluster, DuplicateClusterMember, DuplicateScanReport,
    MemoryConsolidateResult,
};
use super::{
    CreatedBy, DurableMemoryDocument, DurableMemoryFrontmatter, DurableMemoryRelations,
    DurableMemoryRetrieval, DurableMemorySource, DurableMemoryStatus, DurableMemoryType,
    MemoryScope, SessionState, TemporalGranularity,
};

/// Hard structural cap on a durable memory body (characters). Appends that would
/// push a body past this are refused: the auto-merge path falls back to creating a
/// new atomic memory, and the explicit `merge` path fails loudly. This makes it
/// structurally impossible for a memory to grow into a multi-topic / transcript
/// "blob". Purely deterministic — no model or embedding involved.
const MAX_DURABLE_MEMORY_BODY_CHARS: usize = 4000;

/// Separator inserted between accreted sections of a durable memory body. The
/// merge/append paths write it and the blob prefilter counts it, so both always
/// agree on what an "accretion" is.
const MEMORY_SECTION_SEPARATOR: &str = "\n\n---\n\n";

/// Write-time upsert thresholds over IDF-weighted cosine similarity (L2), in
/// [0, 1]. Deliberately precision-biased: `MERGE` is high because a merge is lossy
/// (it grows a memory and can't be cheaply undone), so ONLY a near-identical
/// restatement auto-merges at write time; a same-topic-but-reworded write stays a
/// separate memory (linked), leaving any genuine consolidation to L3's
/// model-gated, reversible pass. `RELATE` is lower because a link is cheap and
/// reversible. A write whose top similarity is `>= MERGE` appends into that
/// memory; `>= RELATE` (but below merge) creates a new atomic memory linked to the
/// near-dups; below `RELATE` is a plain new memory. Calibrated against IDF-cosine
/// samples: near-identical restatement ~0.96, same-fact-reworded ~0.49,
/// topically-unrelated <0.1.
const MERGE_SIMILARITY: f64 = 0.6;
const RELATE_SIMILARITY: f64 = 0.3;
/// Cap on `relations.related` links a single write adds, so a write into a dense
/// cluster doesn't fan out to every neighbor.
const MAX_RELATED_LINKS: usize = 3;

/// How an incoming write relates to the scope's existing memories (L2 upsert).
enum WriteSimilarity {
    /// Strong duplicate — append the incoming content into this existing memory.
    /// Boxed: a full document dwarfs the other variants.
    Merge(Box<DurableMemoryDocument>),
    /// Related but distinct — create a new atomic memory linked to these ids.
    Relate(Vec<String>),
    /// No meaningful similarity — create a plain new atomic memory.
    None,
}

/// Heuristic "keep value" of a memory for capacity eviction (L5), higher = more
/// worth keeping: `recency(updated_at) × confidence × stale_penalty ×
/// access_multiplier`. Deterministic — no model, no embedding.
///
/// `access_multiplier` is the RFC's access-frequency term (#264, follow-up to
/// #263 which shipped this function without it): recall can't write
/// `last_accessed_at` back onto the recalled document without forcing a full
/// per-scope index rebuild on that hot read path, so instead recall appends
/// `{id, ts}` to a cheap per-scope `access_log.jsonl`
/// ([`MemoryStore::record_memory_accesses`]) that the capacity gardener
/// aggregates lazily into this multiplier ([`access_log::access_multiplier`]).
/// Callers with no access-log data for a doc — or that don't wire the log at all
/// — pass `1.0`, which is a true no-op here and reproduces the pre-#264 score
/// exactly (back-compat).
fn memory_value(
    doc: &DurableMemoryDocument,
    now: chrono::DateTime<chrono::Utc>,
    access_multiplier: f64,
) -> f64 {
    let confidence = match doc.frontmatter.confidence.as_deref() {
        Some("high") => 1.0,
        Some("low") => 0.25,
        _ => 0.5, // "medium" or unset
    };
    // Unparseable timestamp → treat as very old (low recency) so it evicts first.
    let age_days = parse_rfc3339(&doc.frontmatter.updated_at)
        .map(|ts| (now - ts).num_days().max(0) as f64)
        .unwrap_or(3650.0);
    let recency = 1.0 / (1.0 + age_days / 30.0);
    let stale_penalty = if matches!(doc.frontmatter.status, DurableMemoryStatus::Stale) {
        0.5
    } else {
        1.0
    };
    confidence * recency * stale_penalty * access_multiplier
}

/// Projected body length (chars) after appending `content` to `body` with the
/// Serialize `value` to pretty JSON bytes, mapping a serialization failure onto
/// an `io::Error` so callers stay on `io::Result`. Shared by the single-file
/// [`MemoryStore::write_json_file`] and the batched refresh so both encode
/// artifacts identically.
fn json_pretty_bytes<T: serde::Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec_pretty(value).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize json: {error}"),
        )
    })
}

/// section separator. Mirrors the real append, which trims trailing whitespace
/// first, so the structural blob guard estimates exactly, not conservatively.
fn projected_merged_body_chars(body: &str, content: &str) -> usize {
    body.trim_end().chars().count()
        + MEMORY_SECTION_SEPARATOR.chars().count()
        + content.chars().count()
}

/// Process-global registry of per-scope write locks.
///
/// `MemoryStore` is cheap and constructed fresh at most call sites (one per HTTP
/// handler, per gardener pass, per auto-dream task), so a lock field on the struct
/// would NOT serialize concurrent writers — each ephemeral instance would get its
/// own mutex. The actual concurrency is many `MemoryStore` instances inside the
/// SAME process (the `bamboo serve` server) racing on the same on-disk scope. A
/// process-global registry keyed by the scope's unique on-disk root therefore
/// serializes all of them.
///
/// Cross-process concurrency to one data dir is not a supported deployment (bodhi
/// runs a single `bamboo serve` sidecar), so in-process locking is sufficient and
/// avoids the complexity / portability cost of OS advisory file locks. The key is
/// the scope root `PathBuf`, which is globally unique across data dir + scope +
/// project, so two stores pointed at the same data dir share locks and two pointed
/// at different dirs (e.g. tests) do not contend.
fn scope_locks() -> &'static DashMap<PathBuf, Arc<Mutex<()>>> {
    static SCOPE_LOCKS: OnceLock<DashMap<PathBuf, Arc<Mutex<()>>>> = OnceLock::new();
    SCOPE_LOCKS.get_or_init(DashMap::new)
}

#[derive(Debug, Clone)]
pub struct MemoryStore {
    resolver: MemoryPathResolver,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl MemoryStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            resolver: MemoryPathResolver::from_data_dir(data_dir),
        }
    }

    pub fn with_defaults() -> Self {
        Self {
            resolver: MemoryPathResolver::from_data_dir(bamboo_config::paths::bamboo_dir()),
        }
    }

    pub fn resolver(&self) -> &MemoryPathResolver {
        &self.resolver
    }

    /// Return the process-global write lock guarding the given scope's on-disk
    /// state. Callers acquire it for the full duration of a read-modify-write +
    /// `refresh_scope_artifacts` critical section so concurrent writers to the same
    /// scope are serialized — the scope index/artifacts can never be left
    /// half-written or inconsistent (the #32 corruption).
    ///
    /// The five resolve-then-lock methods (archive/split/consolidate/contradict/
    /// merge) resolve the target document once to find its scope, then RE-READ it
    /// under the lock before mutating (#235), so two concurrent edits to the SAME
    /// memory id can't lose an update: the second edit sees the first's committed
    /// state rather than a stale pre-lock snapshot. See the
    /// `concurrent_same_memory_edits_do_not_lose_updates` regression test.
    ///
    /// The registry holds one `Arc<Mutex<()>>` per distinct scope root forever
    /// (never evicted), which is negligible: a scope root is global / sessions /
    /// per-project, so the set is tiny and bounded in practice.
    ///
    /// Each mutating public method acquires AT MOST this one `scope_lock` and
    /// never holds it while calling another lock-acquiring PUBLIC method, so there
    /// is no lock nesting across the public API and deadlock is structurally
    /// impossible there.
    ///
    /// One documented exception: `enforce_scope_capacity` (the L5 capacity
    /// gardener) holds `scope_lock` for its whole read-modify-write critical
    /// section and, while holding it, calls the private `access_log_stats` →
    /// `read_access_log_stats_at`, which acquires a SEPARATE `path_lock` scoped to
    /// that scope's `access_log.jsonl` file (see `record_memory_accesses_inner`'s
    /// doc comment for why the access log gets its own lock instead of reusing
    /// `scope_lock`: so a burst of concurrent recalls appending access-log entries
    /// never contends with concurrent scope writers). This IS lock nesting, but it
    /// is deadlock-safe because the acquisition order is fixed and never reversed
    /// anywhere in this file: every path that needs both locks takes `scope_lock`
    /// first and an access-log `path_lock` second; nothing acquires an access-log
    /// `path_lock` and then tries to acquire a `scope_lock` while still holding it
    /// (`record_memory_accesses`, the other access-log caller, never touches
    /// `scope_lock` at all — it's invoked from the lock-free `query_scope` read
    /// path). A fixed, never-reversed acquisition order rules out the circular
    /// wait a deadlock requires.
    fn scope_lock(&self, scope: MemoryScope, project_key: Option<&str>) -> Arc<Mutex<()>> {
        self.path_lock(self.resolver.scope_root(scope, project_key))
    }

    /// Shared per-path mutex from the global registry — the serialization
    /// primitive behind [`scope_lock`] and per-session-topic locking.
    fn path_lock(&self, key: PathBuf) -> Arc<Mutex<()>> {
        scope_locks()
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn derive_project_key_from_workspace(workspace: Option<&Path>) -> Option<String> {
        workspace.map(project_key_from_path)
    }

    pub fn project_key_for_session(&self, session_id: Option<&str>) -> Option<String> {
        let workspace = session_id
            .and_then(workspace_state::get_workspace)
            .or_else(workspace_state::get_configured_default_workspace);
        workspace.map(|path| project_key_from_path(&path))
    }

    pub async fn read_session_topic(
        &self,
        session_id: &str,
        topic: &str,
    ) -> io::Result<Option<String>> {
        self.maybe_migrate_legacy_session(session_id).await?;
        let path = self.session_topic_path(session_id, topic)?;
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(path).await?;
        Ok(Some(content))
    }

    pub async fn write_session_topic(
        &self,
        session_id: &str,
        topic: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
        self.maybe_migrate_legacy_session(session_id).await?;
        let path = self.session_topic_path(session_id, topic)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        crate::atomic_fs::atomic_write(&path, content.as_bytes()).await?;
        self.persist_session_state(session_id).await?;
        Ok(path)
    }

    pub async fn append_session_topic(
        &self,
        session_id: &str,
        topic: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
        // Serialize the read-modify-write so two concurrent appends to the same
        // (session, topic) can't both read the old value and clobber each other
        // (#235). Keyed on the topic path (stable across both callers).
        let lock = self.path_lock(self.session_topic_path(session_id, topic)?);
        let _guard = lock.lock().await;
        let existing = self.read_session_topic(session_id, topic).await?;
        let next = match existing {
            Some(prev) if !prev.trim().is_empty() => format!("{}\n\n{}", prev.trim_end(), content),
            _ => content.to_string(),
        };
        self.write_session_topic(session_id, topic, &next).await
    }

    pub async fn delete_session_topic(&self, session_id: &str, topic: &str) -> io::Result<bool> {
        self.maybe_migrate_legacy_session(session_id).await?;
        let path = self.session_topic_path(session_id, topic)?;
        let deleted = if path.exists() {
            fs::remove_file(&path).await?;
            true
        } else {
            false
        };
        self.persist_session_state(session_id).await?;
        Ok(deleted)
    }

    pub async fn list_session_topics(&self, session_id: &str) -> io::Result<Vec<String>> {
        self.maybe_migrate_legacy_session(session_id).await?;
        self.list_session_topics_without_migration(session_id).await
    }

    async fn list_session_topics_without_migration(
        &self,
        session_id: &str,
    ) -> io::Result<Vec<String>> {
        validate_session_id(session_id)?;
        let dir = self.resolver.session_note_dir(session_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut topics = Vec::new();
        let mut entries = fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "md") {
                if let Some(stem) = path.file_stem().and_then(|value| value.to_str()) {
                    topics.push(stem.to_string());
                }
            }
        }
        topics.sort();
        Ok(topics)
    }

    pub async fn read_session_state(&self, session_id: &str) -> io::Result<SessionState> {
        self.maybe_migrate_legacy_session(session_id).await?;
        validate_session_id(session_id)?;
        let state_path = self.resolver.session_state_path(session_id);
        if state_path.exists() {
            let raw = fs::read_to_string(&state_path).await?;
            if let Ok(state) = serde_json::from_str::<SessionState>(&raw) {
                return Ok(state);
            }
        }

        let now = now_rfc3339();
        Ok(SessionState {
            version: MEMORY_SCHEMA_VERSION,
            session_id: session_id.to_string(),
            created_at: now.clone(),
            updated_at: now,
            last_extracted_at: None,
            last_compacted_at: None,
            topics: self
                .list_session_topics_without_migration(session_id)
                .await?,
        })
    }

    pub async fn read_session_topics_with_content(
        &self,
        session_id: &str,
    ) -> io::Result<Vec<(String, String)>> {
        let topics = self.list_session_topics(session_id).await?;
        let mut out = Vec::new();
        for topic in topics {
            let Some(content) = self.read_session_topic(session_id, &topic).await? else {
                continue;
            };
            let trimmed = content.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push((topic, trimmed.to_string()));
        }
        Ok(out)
    }

    pub async fn mark_session_extracted(
        &self,
        session_id: &str,
        extracted_at: &str,
    ) -> io::Result<()> {
        validate_session_id(session_id)?;
        let mut state = self.read_session_state(session_id).await?;
        state.last_extracted_at = Some(extracted_at.trim().to_string());
        state.updated_at = now_rfc3339();
        let state_path = self.resolver.session_state_path(session_id);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        self.write_json_file(state_path, &state).await
    }

    pub async fn read_dream_view(&self) -> io::Result<Option<String>> {
        self.read_scope_dream_view(MemoryScope::Global, None).await
    }

    pub async fn read_project_dream_view(&self, project_key: &str) -> io::Result<Option<String>> {
        self.read_scope_dream_view(MemoryScope::Project, Some(project_key))
            .await
    }

    pub async fn read_memory_view(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<String>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let path = self
            .resolver
            .views_dir(scope, project_key)
            .join(MEMORY_VIEW_FILE);
        self.read_optional_trimmed_text_file(path).await
    }

    pub async fn read_recent_view(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<String>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let path = self
            .resolver
            .views_dir(scope, project_key)
            .join(RECENT_VIEW_FILE);
        self.read_optional_trimmed_text_file(path).await
    }

    pub async fn read_stale_view(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<String>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let path = self
            .resolver
            .views_dir(scope, project_key)
            .join(STALE_VIEW_FILE);
        self.read_optional_trimmed_text_file(path).await
    }

    pub async fn read_lexical_index(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<LexicalIndex>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let path = self
            .resolver
            .indexes_dir(scope, project_key)
            .join(LEXICAL_INDEX_FILE);
        self.read_optional_json_file(path).await
    }

    pub async fn write_dream_view(&self, content: &str) -> io::Result<PathBuf> {
        self.write_scope_dream_view(MemoryScope::Global, None, content)
            .await
    }

    pub async fn write_project_dream_view(
        &self,
        project_key: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
        self.write_scope_dream_view(MemoryScope::Project, Some(project_key), content)
            .await
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
        let project_key = self.require_project_key(scope, project_key)?;
        let max_chars = options
            .max_chars
            .unwrap_or(DEFAULT_MAX_CHARS)
            .min(MAX_MAX_CHARS);
        let limit = options
            .limit
            .unwrap_or(DEFAULT_QUERY_LIMIT)
            .clamp(1, MAX_QUERY_LIMIT);
        let offset = parse_query_cursor(options.cursor.as_deref());

        let docs = self.list_memory_documents(scope, project_key).await?;
        let mut matches = docs
            .into_iter()
            .filter_map(|doc| {
                let relevance = match_memory_query(
                    &doc,
                    query,
                    filter_types,
                    filter_statuses,
                    filter_granularity,
                )?;
                Some((doc, relevance))
            })
            .collect::<Vec<_>>();

        matches.sort_by(|(left_doc, left_score), (right_doc, right_score)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let left_dt = parse_rfc3339(&left_doc.frontmatter.updated_at)
                        .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
                    let right_dt = parse_rfc3339(&right_doc.frontmatter.updated_at)
                        .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
                    right_dt.cmp(&left_dt)
                })
        });

        let matched_count = matches.len();
        let remaining = matches.into_iter().skip(offset).collect::<Vec<_>>();
        let per_item_max = (max_chars / limit.max(1)).max(120);
        let items = remaining
            .iter()
            .take(limit)
            .map(|(doc, relevance)| MemoryQueryItem {
                id: doc.frontmatter.id.clone(),
                title: doc.frontmatter.title.clone(),
                r#type: doc.frontmatter.r#type,
                scope: doc.frontmatter.scope,
                status: doc.frontmatter.status,
                summary: derive_summary(&doc.body, per_item_max),
                tags: doc.frontmatter.tags.clone(),
                granularity: doc.frontmatter.granularity,
                relevance: (*relevance * 100.0).round() / 100.0,
                related_ids: if options.include_related {
                    Self::combined_related_ids(doc)
                } else {
                    Vec::new()
                },
                project_key: doc.frontmatter.project_key.clone(),
            })
            .collect::<Vec<_>>();
        let returned_count = items.len();
        let remaining_count = remaining.len().saturating_sub(returned_count);
        let next_cursor =
            (remaining_count > 0).then(|| make_query_cursor(scope, offset + returned_count));

        // L5 access signal (#264): log only the docs actually surfaced by this
        // recall (the returned page), not every candidate that merely matched.
        // Best-effort — see `record_memory_accesses` — so a log failure here can
        // never turn a successful recall into an error.
        if !items.is_empty() {
            let accessed_ids: Vec<String> = items.iter().map(|item| item.id.clone()).collect();
            self.record_memory_accesses(scope, project_key, &accessed_ids)
                .await;
        }

        Ok(MemoryQueryResult {
            items,
            returned_count,
            matched_count,
            truncated: remaining_count > 0,
            remaining_count,
            next_cursor,
        })
    }

    pub async fn inspect_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<MemoryInspectResult> {
        let project_key = self.require_project_key(scope, project_key)?;
        let docs = self.list_memory_documents(scope, project_key).await?;
        let mut by_type = BTreeMap::new();
        let mut by_status = BTreeMap::new();
        for doc in &docs {
            *by_type
                .entry(doc.frontmatter.r#type.as_str().to_string())
                .or_insert(0) += 1;
            *by_status
                .entry(doc.frontmatter.status.as_str().to_string())
                .or_insert(0) += 1;
        }
        let views_dir = self.resolver.views_dir(scope, project_key);
        let mut view_files = Vec::new();
        if views_dir.exists() {
            let mut entries = fs::read_dir(&views_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str() {
                    view_files.push(name.to_string());
                }
            }
            view_files.sort();
        }

        let indexes_dir = self.resolver.indexes_dir(scope, project_key);
        let mut index_files = Vec::new();
        if indexes_dir.exists() {
            let mut entries = fs::read_dir(&indexes_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str() {
                    index_files.push(name.to_string());
                }
            }
            index_files.sort();
        }

        let state_dir = self.resolver.state_dir(scope, project_key);
        let mut state_files = Vec::new();
        if state_dir.exists() {
            let mut entries = fs::read_dir(&state_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str() {
                    state_files.push(name.to_string());
                }
            }
            state_files.sort();
        }

        let stale_candidates_path = indexes_dir.join(STALE_CANDIDATES_INDEX_FILE);
        let stale_candidate_count = if stale_candidates_path.exists() {
            fs::read_to_string(&stale_candidates_path)
                .await
                .ok()
                .and_then(|raw| serde_json::from_str::<StaleCandidatesIndex>(&raw).ok())
                .map(|index| index.items.len())
                .unwrap_or(0)
        } else {
            0
        };

        let last_reindex_at = state_dir
            .join("last_reindex.json")
            .exists()
            .then_some(state_dir.join("last_reindex.json"));
        let last_reindex_at = if let Some(path) = last_reindex_at {
            fs::read_to_string(path)
                .await
                .ok()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                .and_then(|value| {
                    value
                        .get("updated_at")
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string)
                })
        } else {
            None
        };

        let dream_path = views_dir.join(DREAM_VIEW_FILE);
        let last_dream_at = if dream_path.exists() {
            fs::metadata(&dream_path)
                .await
                .ok()
                .and_then(|meta| meta.modified().ok())
                .map(chrono::DateTime::<chrono::Utc>::from)
                .map(|dt| dt.to_rfc3339())
        } else {
            None
        };

        Ok(MemoryInspectResult {
            scope,
            project_key: project_key.map(|value| value.to_string()),
            total_memories: docs.len(),
            by_type,
            by_status,
            recent_ids: docs
                .iter()
                .take(10)
                .map(|doc| doc.frontmatter.id.clone())
                .collect(),
            view_files,
            index_files,
            state_files,
            stale_candidate_count,
            last_reindex_at,
            last_dream_at,
            topic_paths: docs
                .iter()
                .map(|doc| bamboo_config::paths::path_to_display_string(&doc.path))
                .collect(),
        })
    }

    pub async fn get_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
    ) -> io::Result<Option<DurableMemoryDocument>> {
        let id = id.trim();
        if id.is_empty() {
            return Ok(None);
        }

        if let Some(project_key) = preferred_project_key {
            if let Some(doc) = self
                .get_memory_in_scope(MemoryScope::Project, Some(project_key), id)
                .await?
            {
                return Ok(Some(doc));
            }
        }

        if let Some(doc) = self
            .get_memory_in_scope(MemoryScope::Global, None, id)
            .await?
        {
            return Ok(Some(doc));
        }

        for project_key in self.list_project_keys().await? {
            if Some(project_key.as_str()) == preferred_project_key {
                continue;
            }
            if let Some(doc) = self
                .get_memory_in_scope(MemoryScope::Project, Some(project_key.as_str()), id)
                .await?
            {
                return Ok(Some(doc));
            }
        }

        Ok(None)
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
        let project_key = self.require_project_key(scope, project_key)?;
        let title = validate_memory_title(title)?;
        let content = content.trim();
        if content.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "content cannot be empty",
            ));
        }

        // Serialize the read-modify-write (find-similar → merge/create → audit →
        // refresh) against concurrent writers to this scope.
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;

        self.ensure_scope_dirs(scope, project_key).await?;
        let tags = normalize_tags(tags.iter().map(String::as_str));

        // L2 write-time upsert: a strong dup appends into the existing memory; a
        // related-but-distinct write becomes a new atomic memory linked to the near
        // dups (reversible, lossless); everything else is a plain new memory.
        let mut related_ids: Vec<String> = Vec::new();
        if allow_merge_if_similar {
            match self
                .find_similar_memory(scope, project_key, r#type, title, content, &tags)
                .await?
            {
                WriteSimilarity::Merge(existing) => {
                    let mut existing = *existing;
                    // Structural guard: never grow a memory into a blob. If appending
                    // would exceed the body cap, don't merge — fall through to a new
                    // atomic memory and instead LINK it to the dup, rather than
                    // silently orphaning it.
                    let already_present = existing.body.contains(content);
                    if already_present
                        || projected_merged_body_chars(&existing.body, content)
                            <= MAX_DURABLE_MEMORY_BODY_CHARS
                    {
                        if !already_present {
                            existing.body = format!(
                                "{}{}{}",
                                existing.body.trim_end(),
                                MEMORY_SECTION_SEPARATOR,
                                content
                            );
                        }
                        existing.frontmatter.updated_at = now_rfc3339();
                        existing.frontmatter.updated_by = CreatedBy {
                            kind: "memory_write".to_string(),
                            id: None,
                            actor: Some(actor.to_string()),
                        };
                        let mut merged_tags = existing.frontmatter.tags.clone();
                        merged_tags.extend(tags.clone());
                        existing.frontmatter.tags =
                            normalize_tags(merged_tags.iter().map(String::as_str));
                        existing.frontmatter.retrieval.keywords = extract_keywords(
                            &existing.frontmatter.title,
                            &existing.body,
                            &existing.frontmatter.tags,
                        );
                        existing.frontmatter.retrieval.entities =
                            detect_entities(&existing.frontmatter.title, &existing.body);
                        self.write_document(&existing).await?;
                        self.append_audit(
                            scope,
                            project_key,
                            MERGE_AUDIT_LOG,
                            AuditLogEntry {
                                timestamp: now_rfc3339(),
                                action: "merge".to_string(),
                                scope,
                                memory_id: Some(existing.frontmatter.id.clone()),
                                session_id: session_id.map(|value| value.to_string()),
                                topic: None,
                                summary: format!(
                                    "Merged new content into existing memory '{}'.",
                                    existing.frontmatter.title
                                ),
                                metadata: Some(serde_json::json!({
                                    "type": existing.frontmatter.r#type.as_str(),
                                    "project_key": project_key,
                                    "allow_merge_if_similar": true,
                                })),
                            },
                        )
                        .await?;
                        self.refresh_scope_artifacts(scope, project_key).await?;
                        return Ok(existing);
                    }
                    // Too big to merge without blobbing → link instead.
                    related_ids = vec![existing.frontmatter.id.clone()];
                }
                WriteSimilarity::Relate(ids) => related_ids = ids,
                WriteSimilarity::None => {}
            }
        }

        let id = self.allocate_memory_id(scope, project_key, title).await?;
        let now = now_rfc3339();
        let project_key_owned = match scope {
            MemoryScope::Project => Some(project_key.unwrap_or("unknown").to_string()),
            _ => None,
        };
        let frontmatter = DurableMemoryFrontmatter {
            id: id.clone(),
            title: title.to_string(),
            r#type,
            scope,
            project_key: project_key_owned,
            granularity,
            status: DurableMemoryStatus::Active,
            freshness: Some("high".to_string()),
            confidence: Some("high".to_string()),
            created_at: now.clone(),
            updated_at: now.clone(),
            created_by: CreatedBy {
                kind: "session".to_string(),
                id: session_id.map(|value| value.to_string()),
                actor: None,
            },
            updated_by: CreatedBy {
                kind: "memory_write".to_string(),
                id: None,
                actor: Some(actor.to_string()),
            },
            sources: session_id
                .map(|value| {
                    vec![DurableMemorySource {
                        kind: "session".to_string(),
                        id: value.to_string(),
                        message_range: Vec::new(),
                    }]
                })
                .unwrap_or_default(),
            relations: DurableMemoryRelations {
                related: related_ids.clone(),
                ..DurableMemoryRelations::default()
            },
            tags: tags.clone(),
            retrieval: DurableMemoryRetrieval {
                keywords: extract_keywords(title, content, &tags),
                entities: detect_entities(title, content),
                embedding_ready: true,
                last_accessed_at: None,
            },
        };
        let doc = DurableMemoryDocument {
            path: self.resolver.topic_path(scope, project_key, &id),
            frontmatter,
            body: content.to_string(),
        };
        self.write_document(&doc).await?;
        self.append_audit(
            scope,
            project_key,
            WRITE_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: "write".to_string(),
                scope,
                memory_id: Some(id.clone()),
                session_id: session_id.map(|value| value.to_string()),
                topic: None,
                summary: format!("Created durable memory '{}'.", title),
                metadata: Some(serde_json::json!({
                    "type": r#type.as_str(),
                    "project_key": project_key,
                    "tags": tags,
                    "related_to": related_ids,
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(scope, project_key).await?;
        Ok(doc)
    }

    pub async fn archive_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        mode: DurableMemoryStatus,
        reason: Option<&str>,
    ) -> io::Result<Option<DurableMemoryDocument>> {
        let Some(mut doc) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        let lock = self.scope_lock(
            doc.frontmatter.scope,
            doc.frontmatter.project_key.as_deref(),
        );
        let _guard = lock.lock().await;
        // Re-read under the lock so the mutation is applied to the latest
        // committed state, not a pre-lock snapshot another writer may have
        // superseded in the meantime (#235).
        let Some(fresh) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        doc = fresh;
        let changed = self
            .set_memory_status(&mut doc, mode, "memory_purge", "main-model")
            .await?;
        self.append_audit(
            doc.frontmatter.scope,
            doc.frontmatter.project_key.as_deref(),
            PURGE_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: mode.as_str().to_string(),
                scope: doc.frontmatter.scope,
                memory_id: Some(doc.frontmatter.id.clone()),
                session_id: None,
                topic: None,
                summary: reason.unwrap_or(mode.as_str()).to_string(),
                metadata: Some(serde_json::json!({
                    "changed": changed,
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(
            doc.frontmatter.scope,
            doc.frontmatter.project_key.as_deref(),
        )
        .await?;
        Ok(Some(doc))
    }

    /// Split a multi-topic "blob" memory into N atomic memories.
    ///
    /// Each piece becomes a new active memory that `supersedes` the original; the
    /// original is then marked `Superseded` (kept for lineage, hidden from recall).
    /// The caller (an LLM) decides the pieces; this only persists them with the
    /// body size cap enforced, so a split can never re-create a blob.
    pub async fn split_memory(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        pieces: &[MemorySplitPiece],
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemorySplitResult>> {
        let Some(mut source) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        if pieces.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "split requires at least one piece",
            ));
        }

        // Validate every piece up front so a bad piece never leaves a partial split.
        for piece in pieces {
            validate_memory_title(&piece.title)?;
            let content = piece.content.trim();
            if content.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "split piece content cannot be empty",
                ));
            }
            if content.chars().count() > MAX_DURABLE_MEMORY_BODY_CHARS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "split piece exceeds the durable memory size cap; make each piece a single atomic fact",
                ));
            }
        }

        let scope = source.frontmatter.scope;
        let project_key_owned = source.frontmatter.project_key.clone();
        let project_key = project_key_owned.as_deref();
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;
        // Re-read under the lock to avoid splitting a stale pre-lock snapshot (#235).
        let Some(fresh) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        source = fresh;
        let source_id = source.frontmatter.id.clone();
        let source_type = source.frontmatter.r#type;
        let source_confidence = source.frontmatter.confidence.clone();
        let source_granularity = source.frontmatter.granularity;
        let source_sources = source.frontmatter.sources.clone();

        // Pieces were fully validated above; this pass only persists them.
        let mut new_ids = Vec::with_capacity(pieces.len());
        for piece in pieces {
            let title = validate_memory_title(&piece.title)?;
            let content = piece.content.trim();
            let r#type = piece.r#type.unwrap_or(source_type);
            let tags = normalize_tags(piece.tags.iter().map(String::as_str));

            let new_id = self.allocate_memory_id(scope, project_key, title).await?;
            let now = now_rfc3339();
            let project_key_field = match scope {
                MemoryScope::Project => Some(project_key.unwrap_or("unknown").to_string()),
                _ => None,
            };
            let frontmatter = DurableMemoryFrontmatter {
                id: new_id.clone(),
                title: title.to_string(),
                r#type,
                scope,
                project_key: project_key_field,
                granularity: source_granularity,
                status: DurableMemoryStatus::Active,
                freshness: Some("high".to_string()),
                confidence: source_confidence.clone(),
                created_at: now.clone(),
                updated_at: now.clone(),
                created_by: CreatedBy {
                    kind: "memory_split".to_string(),
                    id: session_id.map(|value| value.to_string()),
                    actor: Some(actor.to_string()),
                },
                updated_by: CreatedBy {
                    kind: "memory_split".to_string(),
                    id: None,
                    actor: Some(actor.to_string()),
                },
                sources: source_sources.clone(),
                relations: DurableMemoryRelations {
                    supersedes: vec![source_id.clone()],
                    ..DurableMemoryRelations::default()
                },
                tags: tags.clone(),
                retrieval: DurableMemoryRetrieval {
                    keywords: extract_keywords(title, content, &tags),
                    entities: detect_entities(title, content),
                    embedding_ready: true,
                    last_accessed_at: None,
                },
            };
            let doc = DurableMemoryDocument {
                path: self.resolver.topic_path(scope, project_key, &new_id),
                frontmatter,
                body: content.to_string(),
            };
            self.write_document(&doc).await?;
            new_ids.push(new_id);
        }

        self.set_memory_status(
            &mut source,
            DurableMemoryStatus::Superseded,
            "memory_split",
            actor,
        )
        .await?;

        self.append_audit(
            scope,
            project_key,
            MERGE_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: "split".to_string(),
                scope,
                memory_id: Some(source_id.clone()),
                session_id: session_id.map(|value| value.to_string()),
                topic: None,
                summary: format!(
                    "Split memory '{}' into {} atomic memories.",
                    source.frontmatter.title,
                    new_ids.len()
                ),
                metadata: Some(serde_json::json!({
                    "project_key": project_key,
                    "new_ids": new_ids,
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(scope, project_key).await?;

        Ok(Some(MemorySplitResult {
            source_id,
            target_scope: scope,
            project_key: project_key_owned.clone(),
            new_ids,
        }))
    }

    /// Find existing durable memories most lexically similar to a candidate, for
    /// duplicate review. Returns a ranked shortlist (content-keyword Jaccard); it
    /// NEVER merges — the caller (an LLM) judges sameness and then writes/merges/
    /// splits explicitly. Embedding-free: deterministic recall, LLM precision.
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
        let project_key = self.require_project_key(scope, project_key)?;
        let candidate_keywords: HashSet<String> =
            extract_keywords(title, content, tags).into_iter().collect();
        if candidate_keywords.is_empty() {
            return Ok(Vec::new());
        }
        let docs = self.list_memory_documents(scope, project_key).await?;
        let mut scored: Vec<MemoryDuplicateCandidate> = docs
            .into_iter()
            .filter(|doc| {
                doc.frontmatter.status == DurableMemoryStatus::Active
                    && r#type.map_or(true, |wanted| doc.frontmatter.r#type == wanted)
            })
            .filter_map(|doc| {
                let doc_keywords: HashSet<String> =
                    doc.frontmatter.retrieval.keywords.iter().cloned().collect();
                let intersection = candidate_keywords.intersection(&doc_keywords).count();
                if intersection == 0 {
                    return None;
                }
                let union = candidate_keywords.union(&doc_keywords).count();
                let score = (intersection as f64 / union as f64 * 100.0).round() / 100.0;
                Some(MemoryDuplicateCandidate {
                    id: doc.frontmatter.id.clone(),
                    title: doc.frontmatter.title.clone(),
                    r#type: doc.frontmatter.r#type,
                    scope: doc.frontmatter.scope,
                    score,
                    snippet: derive_summary(&doc.body, 200),
                })
            })
            .collect();
        scored.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        scored.truncate(limit.max(1));
        Ok(scored)
    }

    /// Deterministic blob prefilter (NO LLM): flag active memories that look like
    /// multi-topic / transcript "blobs" — `min_appended_sections`+ `---` accretions
    /// or a body over the size cap — ranked worst-first. This is the free, always-on
    /// half of the gardener; the returned items are the worklist for LLM-driven split.
    pub async fn scan_blob_candidates(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        min_appended_sections: usize,
        limit: usize,
    ) -> io::Result<BlobScanReport> {
        let project_key = self.require_project_key(scope, project_key)?;
        let docs = self.list_memory_documents(scope, project_key).await?;
        let scanned = docs.len();
        let mut items: Vec<BlobScanItem> = docs
            .into_iter()
            .filter(|doc| doc.frontmatter.status == DurableMemoryStatus::Active)
            .filter_map(|doc| {
                let appended_sections = doc
                    .body
                    .split(MEMORY_SECTION_SEPARATOR)
                    .count()
                    .saturating_sub(1);
                let body_chars = doc.body.chars().count();
                let over_cap = body_chars > MAX_DURABLE_MEMORY_BODY_CHARS;
                if appended_sections >= min_appended_sections || over_cap {
                    Some(BlobScanItem {
                        id: doc.frontmatter.id.clone(),
                        title: doc.frontmatter.title.clone(),
                        appended_sections,
                        body_chars,
                        over_cap,
                    })
                } else {
                    None
                }
            })
            .collect();
        items.sort_by(|left, right| {
            right
                .appended_sections
                .cmp(&left.appended_sections)
                .then(right.body_chars.cmp(&left.body_chars))
                .then_with(|| left.id.cmp(&right.id))
        });
        let flagged = items.len();
        items.truncate(limit.max(1));
        Ok(BlobScanReport {
            scope,
            project_key: project_key.map(ToString::to_string),
            scanned,
            flagged,
            threshold: min_appended_sections,
            items,
        })
    }

    /// Deterministic dedup prefilter (NO LLM): cluster active memories that look
    /// like near-duplicates (pairwise content-keyword Jaccard ≥ `min_score`),
    /// ranked worst-first. Greedy seeded clustering keeps each memory in at most
    /// one cluster. This is the free, always-on half of the dedup gardener; each
    /// cluster is a worklist item for LLM-driven consolidation.
    pub async fn scan_duplicate_clusters(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        min_score: f64,
        max_members_per_cluster: usize,
        limit: usize,
    ) -> io::Result<DuplicateScanReport> {
        let project_key = self.require_project_key(scope, project_key)?;
        let min_score = min_score.clamp(0.0, 1.0);
        let max_members = max_members_per_cluster.max(2);

        let docs = self.list_memory_documents(scope, project_key).await?;
        let active: Vec<DurableMemoryDocument> = docs
            .into_iter()
            .filter(|doc| doc.frontmatter.status == DurableMemoryStatus::Active)
            .collect();
        let scanned = active.len();

        // Precompute each memory's keyword set once (reused across all pair checks).
        let keyword_sets: Vec<HashSet<String>> = active
            .iter()
            .map(|doc| doc.frontmatter.retrieval.keywords.iter().cloned().collect())
            .collect();

        let mut used = vec![false; active.len()];
        let mut clusters: Vec<DuplicateCluster> = Vec::new();
        let mut clustered = 0usize;
        for i in 0..active.len() {
            if used[i] || keyword_sets[i].is_empty() {
                continue;
            }
            // Collect everything similar to seed i, with its score, then keep the
            // strongest partners up to the per-cluster cap.
            let mut partners: Vec<(usize, f64)> = Vec::new();
            for j in (i + 1)..active.len() {
                if used[j] || keyword_sets[j].is_empty() {
                    continue;
                }
                let intersection = keyword_sets[i].intersection(&keyword_sets[j]).count();
                if intersection == 0 {
                    continue;
                }
                let union = keyword_sets[i].union(&keyword_sets[j]).count();
                let score = intersection as f64 / union as f64;
                if score >= min_score {
                    partners.push((j, score));
                }
            }
            if partners.is_empty() {
                continue;
            }
            partners.sort_by(|left, right| {
                right
                    .1
                    .partial_cmp(&left.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        active[left.0]
                            .frontmatter
                            .id
                            .cmp(&active[right.0].frontmatter.id)
                    })
            });
            partners.truncate(max_members.saturating_sub(1));
            let max_score = partners.first().map(|(_, score)| *score).unwrap_or(0.0);

            let mut member_indices = vec![i];
            member_indices.extend(partners.iter().map(|(idx, _)| *idx));
            for &idx in &member_indices {
                used[idx] = true;
            }
            clustered += member_indices.len();

            let members = member_indices
                .iter()
                .map(|&idx| {
                    let doc = &active[idx];
                    DuplicateClusterMember {
                        id: doc.frontmatter.id.clone(),
                        title: doc.frontmatter.title.clone(),
                        r#type: doc.frontmatter.r#type,
                        snippet: derive_summary(&doc.body, 200),
                    }
                })
                .collect();
            clusters.push(DuplicateCluster {
                members,
                max_score: (max_score * 100.0).round() / 100.0,
            });
        }

        clusters.sort_by(|left, right| {
            right
                .max_score
                .partial_cmp(&left.max_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(right.members.len().cmp(&left.members.len()))
                .then_with(|| left.members[0].id.cmp(&right.members[0].id))
        });
        clusters.truncate(limit.max(1));

        Ok(DuplicateScanReport {
            scope,
            project_key: project_key.map(ToString::to_string),
            scanned,
            clustered,
            threshold: min_score,
            clusters,
        })
    }

    /// Consolidate N near-duplicate memories into ONE canonical atomic memory.
    ///
    /// The merged piece (decided by the caller, an LLM) becomes a new active memory
    /// that `supersedes` every source; each source is then marked `Superseded`
    /// (kept for lineage, hidden from recall). The body size cap is enforced, so a
    /// consolidation can never produce a blob. This is the inverse of `split_memory`.
    pub async fn consolidate_memories(
        &self,
        ids: &[String],
        preferred_project_key: Option<&str>,
        merged: &MemorySplitPiece,
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemoryConsolidateResult>> {
        if ids.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "consolidate requires at least two source memories",
            ));
        }

        // Validate the merged piece up front so nothing is written on a bad payload.
        let title = validate_memory_title(&merged.title)?;
        let content = merged.content.trim();
        if content.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "consolidated content cannot be empty",
            ));
        }
        if content.chars().count() > MAX_DURABLE_MEMORY_BODY_CHARS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "consolidated memory exceeds the durable memory size cap; keep it a single atomic fact",
            ));
        }

        // Resolve every source first; a missing one aborts before any write. Dedupe
        // ids so a repeated id can't supersede itself twice.
        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut sources: Vec<DurableMemoryDocument> = Vec::with_capacity(ids.len());
        for id in ids {
            let id = id.trim();
            if !seen_ids.insert(id.to_string()) {
                continue;
            }
            let Some(doc) = self.get_memory(id, preferred_project_key).await? else {
                return Ok(None);
            };
            sources.push(doc);
        }
        if sources.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "consolidate requires at least two distinct source memories",
            ));
        }

        let scope = sources[0].frontmatter.scope;
        let project_key_owned = sources[0].frontmatter.project_key.clone();
        if sources.iter().any(|doc| {
            doc.frontmatter.scope != scope || doc.frontmatter.project_key != project_key_owned
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "all consolidated memories must share one scope and project",
            ));
        }
        let project_key = project_key_owned.as_deref();
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;
        // Re-read every source under the lock: the supersede writes below persist
        // each source doc, so operating on pre-lock snapshots would clobber a
        // concurrent mutation to any of them (#235). ids/scope are stable.
        let mut fresh_sources = Vec::with_capacity(sources.len());
        for source in &sources {
            let Some(doc) = self
                .get_memory(&source.frontmatter.id, preferred_project_key)
                .await?
            else {
                return Ok(None);
            };
            fresh_sources.push(doc);
        }
        sources = fresh_sources;
        let superseded_ids: Vec<String> = sources
            .iter()
            .map(|doc| doc.frontmatter.id.clone())
            .collect();

        let r#type = merged.r#type.unwrap_or(sources[0].frontmatter.r#type);
        let tags = normalize_tags(merged.tags.iter().map(String::as_str));
        let new_id = self.allocate_memory_id(scope, project_key, title).await?;
        let now = now_rfc3339();
        let project_key_field = match scope {
            MemoryScope::Project => Some(project_key.unwrap_or("unknown").to_string()),
            _ => None,
        };
        // Union the provenance of every source so the consolidated memory keeps the
        // full lineage of where its facts came from.
        let mut sources_union: Vec<DurableMemorySource> = Vec::new();
        for doc in &sources {
            for source in &doc.frontmatter.sources {
                if !sources_union.contains(source) {
                    sources_union.push(source.clone());
                }
            }
        }
        // Consolidated memory inherits the coarsest (most cache-stable) granularity
        // among its sources, so merging never makes a long-lived fact look more
        // volatile than it was. Sources without a granularity are ignored here.
        // (Day < Week < Month < Quarter < Year by derived Ord, so `max` is coarsest.)
        let granularity = sources
            .iter()
            .filter_map(|doc| doc.frontmatter.granularity)
            .max();
        let frontmatter = DurableMemoryFrontmatter {
            id: new_id.clone(),
            title: title.to_string(),
            r#type,
            scope,
            project_key: project_key_field,
            granularity,
            status: DurableMemoryStatus::Active,
            freshness: Some("high".to_string()),
            confidence: sources[0].frontmatter.confidence.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
            created_by: CreatedBy {
                kind: "memory_consolidate".to_string(),
                id: session_id.map(|value| value.to_string()),
                actor: Some(actor.to_string()),
            },
            updated_by: CreatedBy {
                kind: "memory_consolidate".to_string(),
                id: None,
                actor: Some(actor.to_string()),
            },
            sources: sources_union,
            relations: DurableMemoryRelations {
                supersedes: superseded_ids.clone(),
                ..DurableMemoryRelations::default()
            },
            tags: tags.clone(),
            retrieval: DurableMemoryRetrieval {
                keywords: extract_keywords(title, content, &tags),
                entities: detect_entities(title, content),
                embedding_ready: true,
                last_accessed_at: None,
            },
        };
        let doc = DurableMemoryDocument {
            path: self.resolver.topic_path(scope, project_key, &new_id),
            frontmatter,
            body: content.to_string(),
        };
        self.write_document(&doc).await?;

        for mut source in sources {
            self.set_memory_status(
                &mut source,
                DurableMemoryStatus::Superseded,
                "memory_consolidate",
                actor,
            )
            .await?;
        }

        self.append_audit(
            scope,
            project_key,
            MERGE_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: "consolidate".to_string(),
                scope,
                memory_id: Some(new_id.clone()),
                session_id: session_id.map(|value| value.to_string()),
                topic: None,
                summary: format!(
                    "Consolidated {} near-duplicate memories into '{}'.",
                    superseded_ids.len(),
                    title
                ),
                metadata: Some(serde_json::json!({
                    "project_key": project_key,
                    "superseded_ids": superseded_ids,
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(scope, project_key).await?;

        Ok(Some(MemoryConsolidateResult {
            new_id,
            target_scope: scope,
            project_key: project_key_owned.clone(),
            superseded_ids,
        }))
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
        let project_key = self.require_project_key(scope, project_key)?;
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;
        let mut docs = self.list_memory_documents(scope, project_key).await?;
        let mut updated_ids = Vec::new();
        for doc in &mut docs {
            if match_memory_query(doc, None, filter_types, filter_statuses, filter_granularity)
                .is_none()
            {
                continue;
            }
            let changed = self
                .set_memory_status(doc, mode, "memory_purge", "main-model")
                .await?;
            if changed {
                updated_ids.push(doc.frontmatter.id.clone());
            }
        }

        let updated_ids_for_audit = updated_ids.clone();
        self.append_audit(
            scope,
            project_key,
            PURGE_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: mode.as_str().to_string(),
                scope,
                memory_id: None,
                session_id: None,
                topic: None,
                summary: reason.unwrap_or(mode.as_str()).to_string(),
                metadata: Some(serde_json::json!({
                    "project_key": project_key,
                    "matched_count": updated_ids_for_audit.len(),
                    "updated_ids": updated_ids_for_audit,
                    "type_filters": filter_types.map(|values| values.iter().map(|value| value.as_str()).collect::<Vec<_>>()),
                    "status_filters": filter_statuses.map(|values| values.iter().map(|value| value.as_str()).collect::<Vec<_>>()),
                    "granularity_filters": filter_granularity.map(|values| values.iter().map(|value| value.as_str()).collect::<Vec<_>>()),
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(scope, project_key).await?;

        Ok(MemoryPurgeResult {
            scope,
            project_key: project_key.map(ToString::to_string),
            mode,
            matched_count: updated_ids.len(),
            updated_ids,
        })
    }

    pub async fn mark_memory_contradicted(
        &self,
        id: &str,
        preferred_project_key: Option<&str>,
        contradicted_by_ids: &[String],
        reason: Option<&str>,
        session_id: Option<&str>,
        actor: &str,
    ) -> io::Result<Option<MemoryContradictionResult>> {
        let Some(mut target) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        let lock = self.scope_lock(
            target.frontmatter.scope,
            target.frontmatter.project_key.as_deref(),
        );
        let _guard = lock.lock().await;
        // Re-read under the lock so a concurrent mutation isn't clobbered (#235).
        let Some(fresh) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        target = fresh;

        let requested_ids: Vec<String> = contradicted_by_ids
            .iter()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .filter(|value| *value != target.frontmatter.id)
            .map(ToString::to_string)
            .collect();

        let mut contradicted_ids = Vec::new();
        let mut missing_ids = Vec::new();
        let mut changed = false;
        let mut contradicted_by = target.frontmatter.relations.contradicted_by.clone();

        for source_id in requested_ids {
            if self
                .get_memory_in_scope(
                    target.frontmatter.scope,
                    target.frontmatter.project_key.as_deref(),
                    &source_id,
                )
                .await?
                .is_some()
            {
                if !contradicted_by.contains(&source_id) {
                    contradicted_by.push(source_id.clone());
                    changed = true;
                }
                contradicted_ids.push(source_id);
            } else {
                missing_ids.push(source_id);
            }
        }

        contradicted_ids.sort();
        contradicted_ids.dedup();
        missing_ids.sort();
        missing_ids.dedup();
        contradicted_by.sort();
        contradicted_by.dedup();
        target.frontmatter.relations.contradicted_by = contradicted_by;
        if !contradicted_ids.is_empty()
            && target.frontmatter.status != DurableMemoryStatus::Contradicted
        {
            self.set_memory_status(
                &mut target,
                DurableMemoryStatus::Contradicted,
                "memory_contradiction",
                actor,
            )
            .await?;
            changed = true;
        } else if changed {
            target.frontmatter.updated_at = now_rfc3339();
            target.frontmatter.updated_by = CreatedBy {
                kind: "memory_contradiction".to_string(),
                id: None,
                actor: Some(actor.to_string()),
            };
            self.write_document(&target).await?;
        }

        let contradicted_ids_for_audit = contradicted_ids.clone();
        let missing_ids_for_audit = missing_ids.clone();
        self.append_audit(
            target.frontmatter.scope,
            target.frontmatter.project_key.as_deref(),
            CONTRADICTION_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: "contradict".to_string(),
                scope: target.frontmatter.scope,
                memory_id: Some(target.frontmatter.id.clone()),
                session_id: session_id.map(ToString::to_string),
                topic: None,
                summary: reason.unwrap_or("marked contradicted").to_string(),
                metadata: Some(serde_json::json!({
                    "project_key": target.frontmatter.project_key,
                    "changed": changed,
                    "contradicted_by_ids": contradicted_ids_for_audit,
                    "missing_ids": missing_ids_for_audit,
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(
            target.frontmatter.scope,
            target.frontmatter.project_key.as_deref(),
        )
        .await?;

        Ok(Some(MemoryContradictionResult {
            target_id: target.frontmatter.id.clone(),
            target_scope: target.frontmatter.scope,
            project_key: target.frontmatter.project_key.clone(),
            changed,
            contradicted_ids,
            missing_ids,
            path: target.path.clone(),
        }))
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
        let Some(mut doc) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        let lock = self.scope_lock(
            doc.frontmatter.scope,
            doc.frontmatter.project_key.as_deref(),
        );
        let _guard = lock.lock().await;
        // Re-read under the lock so the merge isn't applied to a stale snapshot (#235).
        let Some(fresh) = self.get_memory(id, preferred_project_key).await? else {
            return Ok(None);
        };
        doc = fresh;

        let content = content.trim();
        if content.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "content cannot be empty",
            ));
        }

        let mut changed = false;
        let mut appended = false;
        let mut tags_updated = false;
        if !doc.body.contains(content) {
            // Structural guard: an explicit merge must not grow a memory into a blob.
            // Fail loudly so the caller consolidates/rewrites into a single coherent
            // statement or creates a separate memory instead of appending.
            if projected_merged_body_chars(&doc.body, content) > MAX_DURABLE_MEMORY_BODY_CHARS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "merge would exceed the durable memory size cap; consolidate the memory into one coherent statement or create a separate memory instead of appending",
                ));
            }
            doc.body = format!(
                "{}{}{}",
                doc.body.trim_end(),
                MEMORY_SECTION_SEPARATOR,
                content
            );
            changed = true;
            appended = true;
        }

        let mut merged_tags = doc.frontmatter.tags.clone();
        let original_tags = merged_tags.clone();
        merged_tags.extend(tags.iter().cloned());
        let normalized_tags = normalize_tags(merged_tags.iter().map(String::as_str));
        if normalized_tags != original_tags {
            doc.frontmatter.tags = normalized_tags;
            changed = true;
            tags_updated = true;
        }

        let source_ids: Vec<String> = source_memory_ids
            .iter()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .filter(|value| value != &doc.frontmatter.id)
            .collect();
        if !source_ids.is_empty() {
            let mut supersedes = doc.frontmatter.relations.supersedes.clone();
            supersedes.extend(source_ids.iter().cloned());
            let mut seen = BTreeMap::<String, ()>::new();
            for value in supersedes {
                seen.insert(value, ());
            }
            let next_supersedes = seen.into_keys().collect::<Vec<_>>();
            if next_supersedes != doc.frontmatter.relations.supersedes {
                doc.frontmatter.relations.supersedes = next_supersedes;
                changed = true;
            }
        }

        doc.frontmatter.updated_at = now_rfc3339();
        doc.frontmatter.updated_by = CreatedBy {
            kind: "memory_merge".to_string(),
            id: None,
            actor: Some(actor.to_string()),
        };
        doc.frontmatter.retrieval.keywords =
            extract_keywords(&doc.frontmatter.title, &doc.body, &doc.frontmatter.tags);
        doc.frontmatter.retrieval.entities = detect_entities(&doc.frontmatter.title, &doc.body);
        self.write_document(&doc).await?;

        let mut superseded_ids = Vec::new();
        for source_id in &source_ids {
            if let Some(mut source_doc) = self
                .get_memory_in_scope(
                    doc.frontmatter.scope,
                    doc.frontmatter.project_key.as_deref(),
                    source_id,
                )
                .await?
            {
                if source_doc.frontmatter.status != DurableMemoryStatus::Superseded {
                    source_doc.frontmatter.status = DurableMemoryStatus::Superseded;
                    source_doc.frontmatter.updated_at = now_rfc3339();
                    source_doc.frontmatter.updated_by = CreatedBy {
                        kind: "memory_merge".to_string(),
                        id: None,
                        actor: Some(actor.to_string()),
                    };
                    self.write_document(&source_doc).await?;
                }
                superseded_ids.push(source_id.clone());
            }
        }

        self.append_audit(
            doc.frontmatter.scope,
            doc.frontmatter.project_key.as_deref(),
            MERGE_AUDIT_LOG,
            AuditLogEntry {
                timestamp: now_rfc3339(),
                action: "merge".to_string(),
                scope: doc.frontmatter.scope,
                memory_id: Some(doc.frontmatter.id.clone()),
                session_id: session_id.map(|value| value.to_string()),
                topic: None,
                summary: format!("Merged content into memory '{}'.", doc.frontmatter.title),
                metadata: Some(serde_json::json!({
                    "project_key": doc.frontmatter.project_key,
                    "changed": changed,
                    "appended": appended,
                    "tags_updated": tags_updated,
                    "source_memory_ids": source_ids,
                    "superseded_ids": superseded_ids,
                })),
            },
        )
        .await?;
        self.refresh_scope_artifacts(
            doc.frontmatter.scope,
            doc.frontmatter.project_key.as_deref(),
        )
        .await?;

        Ok(Some(MemoryMergeResult {
            merged_id: doc.frontmatter.id.clone(),
            target_scope: doc.frontmatter.scope,
            project_key: doc.frontmatter.project_key.clone(),
            changed,
            appended,
            tags_updated,
            superseded_ids,
            path: doc.path.clone(),
        }))
    }

    pub async fn rebuild_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<()> {
        let project_key = self.require_project_key(scope, project_key)?;
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;
        self.refresh_scope_artifacts(scope, project_key).await
    }

    pub async fn list_project_keys(&self) -> io::Result<Vec<String>> {
        let root = self.resolver.scopes_root().join("projects");
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut entries = fs::read_dir(root).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }

    pub async fn list_memory_documents(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Vec<DurableMemoryDocument>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let topic_dir = self.resolver.topic_dir(scope, project_key);
        if !topic_dir.exists() {
            return Ok(Vec::new());
        }

        let mut docs = Vec::new();
        let mut entries = fs::read_dir(topic_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "md") {
                let raw = fs::read_to_string(&path).await?;
                let (frontmatter, body) = parse_markdown_document(&raw)?;
                docs.push(DurableMemoryDocument {
                    frontmatter,
                    body,
                    path,
                });
            }
        }
        sort_memories_desc(&mut docs);
        Ok(docs)
    }

    /// Cheap count of durable-memory topic files in one scope — a readdir with no
    /// parse (unlike [`Self::list_memory_documents`]). Counts every `.md` topic
    /// regardless of status; it is a growth signal, not a recall count.
    pub async fn count_scope_memories(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<usize> {
        let project_key = self.require_project_key(scope, project_key)?;
        let topic_dir = self.resolver.topic_dir(scope, project_key);
        if !topic_dir.exists() {
            return Ok(0);
        }
        let mut count = 0usize;
        let mut entries = fs::read_dir(topic_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().is_some_and(|ext| ext == "md") {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Total durable-memory count across the global scope and every project scope
    /// (cheap; no parse). Used by the volume-triggered maintenance pass (L4) to
    /// detect library growth between time ticks.
    pub async fn count_all_memories(&self) -> io::Result<usize> {
        let mut total = self.count_scope_memories(MemoryScope::Global, None).await?;
        for key in self.list_project_keys().await.unwrap_or_default() {
            total += self
                .count_scope_memories(MemoryScope::Project, Some(&key))
                .await?;
        }
        Ok(total)
    }

    /// Bound a scope's RECALLABLE size to `capacity` by archiving the
    /// lowest-[`memory_value`] memories OUT OF the recall index — capacity via
    /// archive, NEVER delete (L5). Archived docs stay on disk (reversible; a later
    /// pass or the user can restore them) but drop out of recall/scoring.
    ///
    /// Precision-biased and conservative:
    /// - Only `Active`/`Stale` docs count toward `capacity` (already-archived /
    ///   superseded don't).
    /// - Only `Project` memories are evictable; `Reference` (curated), `User`
    ///   (who the user is) and `Feedback` (their preferences) are EXEMPT — they are
    ///   high-value identity/reference facts, never auto-archived.
    /// - At most `max_archivals` are archived per call, so a big overflow is drained
    ///   gradually across runs rather than in one burst.
    /// - `capacity == 0` (or `max_archivals == 0`) is a no-op (feature off).
    ///
    /// Returns the archived ids. One index refresh for the whole batch.
    pub async fn enforce_scope_capacity(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        capacity: usize,
        max_archivals: usize,
    ) -> io::Result<Vec<String>> {
        if capacity == 0 || max_archivals == 0 {
            return Ok(Vec::new());
        }
        let project_key = self.require_project_key(scope, project_key)?;
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;

        let mut docs = self.list_memory_documents(scope, project_key).await?;
        let is_scorable = |status: DurableMemoryStatus| {
            matches!(
                status,
                DurableMemoryStatus::Active | DurableMemoryStatus::Stale
            )
        };
        let scorable = docs
            .iter()
            .filter(|doc| is_scorable(doc.frontmatter.status))
            .count();
        if scorable <= capacity {
            return Ok(Vec::new());
        }
        let overflow = scorable - capacity;
        let now = chrono::Utc::now();

        // Rank evictable (scorable + Project) docs by ascending keep-value.
        let mut evictable: Vec<&DurableMemoryDocument> = docs
            .iter()
            .filter(|doc| {
                is_scorable(doc.frontmatter.status)
                    && doc.frontmatter.r#type == DurableMemoryType::Project
            })
            .collect();

        // Unachievable-bound guard: only Project docs are evictable, so the lowest
        // recallable count we can reach is the exempt (Reference/User/Feedback)
        // scorable count. If that alone already exceeds `capacity`, archiving every
        // Project memory still wouldn't meet the bound — so DON'T strip them all
        // (pointless + destructive). Skip; the capacity is set below the exempt
        // floor and the user should raise it.
        let exempt_scorable = scorable - evictable.len();
        if exempt_scorable > capacity {
            return Ok(Vec::new());
        }

        // Lazily aggregate the recall access log (#264) into the access-frequency
        // multiplier `memory_value` folds in — only read here, on the
        // infrequent gardener pass, never on the hot recall path. Best-effort: a
        // read failure (or no log file yet) degrades to an empty map, which
        // `access_log::access_multiplier` turns into the neutral `1.0` for every
        // doc, i.e. identical to pre-#264 scoring.
        let access_stats = self.access_log_stats(scope, project_key).await;

        // Lowest keep-value first; within a value tie, evict the OLDEST first (keep
        // the newer restatement). `updated_at` is UTC rfc3339 → string order is
        // chronological.
        evictable.sort_by(|a, b| {
            let value_a = memory_value(
                a,
                now,
                access_log::access_multiplier(access_stats.get(&a.frontmatter.id)),
            );
            let value_b = memory_value(
                b,
                now,
                access_log::access_multiplier(access_stats.get(&b.frontmatter.id)),
            );
            value_a
                .partial_cmp(&value_b)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.frontmatter.updated_at.cmp(&b.frontmatter.updated_at))
        });
        let target = overflow.min(max_archivals).min(evictable.len());
        let archive_ids: std::collections::HashSet<String> = evictable
            .iter()
            .take(target)
            .map(|doc| doc.frontmatter.id.clone())
            .collect();
        if archive_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut archived = Vec::with_capacity(archive_ids.len());
        for doc in docs
            .iter_mut()
            .filter(|doc| archive_ids.contains(&doc.frontmatter.id))
        {
            let changed = self
                .set_memory_status(
                    doc,
                    DurableMemoryStatus::Archived,
                    "capacity_eviction",
                    "gardener",
                )
                .await?;
            if changed {
                self.append_audit(
                    scope,
                    project_key,
                    PURGE_AUDIT_LOG,
                    AuditLogEntry {
                        timestamp: now_rfc3339(),
                        action: DurableMemoryStatus::Archived.as_str().to_string(),
                        scope,
                        memory_id: Some(doc.frontmatter.id.clone()),
                        session_id: None,
                        topic: None,
                        summary: "Archived out of recall to enforce scope capacity.".to_string(),
                        metadata: Some(serde_json::json!({
                            "reason": "capacity_eviction",
                            "capacity": capacity,
                        })),
                    },
                )
                .await?;
                archived.push(doc.frontmatter.id.clone());
            }
        }
        if !archived.is_empty() {
            self.refresh_scope_artifacts(scope, project_key).await?;
        }
        Ok(archived)
    }

    /// Freshness gardener (issue #61 phase 2, follow-up to L5/#263's capacity
    /// gardener): conservatively demotes Active day/week-granularity memories to
    /// Stale once they cross the documented staleness window
    /// ([`freshness::granularity_expired`]) — never Archived, never deleted, so a
    /// Stale memory stays recallable (just lower-confidence, see `memory_value`'s
    /// `stale_penalty`) and reversible. Memories with no granularity, or a coarse
    /// one (month/quarter/year), are never touched: the issue explicitly scopes
    /// this to the "high churn" granularities, so nothing is silently
    /// reclassified just for lacking the dimension. Already-Stale/Superseded/
    /// Contradicted/Archived memories are left alone (this pass only ever moves
    /// Active → Stale, once).
    pub async fn expire_stale_granularity(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Vec<String>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;

        let mut docs = self.list_memory_documents(scope, project_key).await?;
        let mut expired = Vec::new();
        for doc in docs
            .iter_mut()
            .filter(|doc| doc.frontmatter.status == DurableMemoryStatus::Active)
            .filter(|doc| {
                freshness::granularity_expired(
                    doc.frontmatter.granularity,
                    &doc.frontmatter.updated_at,
                )
            })
        {
            let changed = self
                .set_memory_status(
                    doc,
                    DurableMemoryStatus::Stale,
                    "granularity_expiry",
                    "gardener",
                )
                .await?;
            if changed {
                self.append_audit(
                    scope,
                    project_key,
                    PURGE_AUDIT_LOG,
                    AuditLogEntry {
                        timestamp: now_rfc3339(),
                        action: DurableMemoryStatus::Stale.as_str().to_string(),
                        scope,
                        memory_id: Some(doc.frontmatter.id.clone()),
                        session_id: None,
                        topic: None,
                        summary: "Marked stale: fine-grained (day/week) memory aged past its temporal-granularity freshness window.".to_string(),
                        metadata: Some(serde_json::json!({
                            "reason": "granularity_expiry",
                            "granularity": doc.frontmatter.granularity.map(TemporalGranularity::as_str),
                        })),
                    },
                )
                .await?;
                expired.push(doc.frontmatter.id.clone());
            }
        }
        if !expired.is_empty() {
            self.refresh_scope_artifacts(scope, project_key).await?;
        }
        Ok(expired)
    }

    /// Classify how an incoming write relates to the scope's existing memories,
    /// using IDF-weighted cosine over field-weighted token bags (L2). Retires the
    /// old raw count-overlap gate: a common shared keyword no longer counts as much
    /// as a rare one, so unrelated memories are not force-merged and genuine dups
    /// (even reworded) are caught.
    async fn find_similar_memory(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        r#type: DurableMemoryType,
        title: &str,
        content: &str,
        tags: &[String],
    ) -> io::Result<WriteSimilarity> {
        let normalized_title = super::sanitize_component(title);
        let docs: Vec<DurableMemoryDocument> = self
            .list_memory_documents(scope, project_key)
            .await?
            .into_iter()
            .filter(|doc| {
                doc.frontmatter.status == DurableMemoryStatus::Active
                    && doc.frontmatter.r#type == r#type
            })
            .collect();

        // Exact (normalized) title match is an unambiguous merge — keep the fast path.
        if let Some(exact) = docs
            .iter()
            .find(|doc| super::sanitize_component(&doc.frontmatter.title) == normalized_title)
        {
            return Ok(WriteSimilarity::Merge(Box::new(exact.clone())));
        }
        // Exact whole-body duplicate: the same content recorded VERBATIM under a
        // different title is an unambiguous dup regardless of similarity score.
        // Merge into it (idempotent — the append path is a no-op when the body
        // already holds the content), so verbatim re-writes never accumulate. This
        // is precision-safe: only a full-body equality, not a substring, matches.
        if let Some(dup) = docs.iter().find(|doc| doc.body.trim() == content) {
            return Ok(WriteSimilarity::Merge(Box::new(dup.clone())));
        }
        if docs.is_empty() {
            return Ok(WriteSimilarity::None);
        }

        // Build field bags for the incoming memory + every candidate, then score
        // with IDF-weighted cosine over the whole compared set (so IDF is meaningful
        // and every term has df >= 1).
        let incoming_bag = lexical_bm25::field_weighted_bag(
            title,
            &extract_keywords(title, content, tags),
            tags,
            &detect_entities(title, content),
            content,
        );
        let candidate_bags: Vec<std::collections::HashMap<String, f64>> = docs
            .iter()
            .map(|doc| {
                lexical_bm25::field_weighted_bag(
                    &doc.frontmatter.title,
                    &doc.frontmatter.retrieval.keywords,
                    &doc.frontmatter.tags,
                    &doc.frontmatter.retrieval.entities,
                    &doc.body,
                )
            })
            .collect();

        let mut all_bags: Vec<&std::collections::HashMap<String, f64>> =
            Vec::with_capacity(candidate_bags.len() + 1);
        all_bags.push(&incoming_bag);
        all_bags.extend(candidate_bags.iter());
        let corpus = lexical_bm25::SimilarityCorpus::build(&all_bags);

        let mut best_merge: Option<(f64, usize)> = None;
        let mut related: Vec<(f64, String)> = Vec::new();
        for (idx, bag) in candidate_bags.iter().enumerate() {
            let sim = corpus.cosine(&incoming_bag, bag);
            if sim >= MERGE_SIMILARITY {
                match best_merge {
                    Some((best, _)) if best >= sim => {}
                    _ => best_merge = Some((sim, idx)),
                }
            } else if sim >= RELATE_SIMILARITY {
                related.push((sim, docs[idx].frontmatter.id.clone()));
            }
        }

        if let Some((_, idx)) = best_merge {
            return Ok(WriteSimilarity::Merge(Box::new(docs[idx].clone())));
        }
        if related.is_empty() {
            return Ok(WriteSimilarity::None);
        }
        // Highest-similarity related first; cap the fan-out.
        related.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        related.truncate(MAX_RELATED_LINKS);
        Ok(WriteSimilarity::Relate(
            related.into_iter().map(|(_, id)| id).collect(),
        ))
    }

    fn combined_related_ids(doc: &DurableMemoryDocument) -> Vec<String> {
        let mut all = doc.frontmatter.relations.related.clone();
        all.extend(doc.frontmatter.relations.supersedes.clone());
        all.extend(doc.frontmatter.relations.contradicted_by.clone());
        all.sort();
        all.dedup();
        all.retain(|value| value != &doc.frontmatter.id);
        all
    }

    async fn set_memory_status(
        &self,
        doc: &mut DurableMemoryDocument,
        status: DurableMemoryStatus,
        kind: &str,
        actor: &str,
    ) -> io::Result<bool> {
        let changed = doc.frontmatter.status != status;
        if !changed {
            return Ok(false);
        }
        doc.frontmatter.status = status;
        doc.frontmatter.updated_at = now_rfc3339();
        doc.frontmatter.updated_by = CreatedBy {
            kind: kind.to_string(),
            id: None,
            actor: Some(actor.to_string()),
        };
        self.write_document(doc).await?;
        Ok(true)
    }

    async fn get_memory_in_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        id: &str,
    ) -> io::Result<Option<DurableMemoryDocument>> {
        let project_key = self.require_project_key(scope, project_key)?;
        let path = self.resolver.topic_path(scope, project_key, id);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(&path).await?;
        let (frontmatter, body) = parse_markdown_document(&raw)?;
        Ok(Some(DurableMemoryDocument {
            frontmatter,
            body,
            path,
        }))
    }

    async fn write_document(&self, doc: &DurableMemoryDocument) -> io::Result<()> {
        if let Some(parent) = doc.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let rendered = render_markdown_document(&doc.frontmatter, &doc.body)?;
        // Atomic write so a crash mid-write can never truncate/corrupt a user
        // memory document (#166, the worst-impact case from #35).
        crate::atomic_fs::atomic_write(&doc.path, rendered.as_bytes()).await
    }

    async fn allocate_memory_id(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        title: &str,
    ) -> io::Result<String> {
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        for counter in 0..1000 {
            let seed = format!("{}:{}:{}", title, timestamp, counter);
            let suffix = short_stable_hash(&seed).unwrap_or_else(|| "00000000".to_string());
            let id = format!("mem_{}_{}", timestamp, &suffix[..6.min(suffix.len())]);
            let path = self.resolver.topic_path(scope, project_key, &id);
            if !path.exists() {
                return Ok(id);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "failed to allocate a unique memory id",
        ))
    }

    async fn refresh_scope_artifacts(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<()> {
        let project_key = self.require_project_key(scope, project_key)?;
        self.ensure_scope_dirs(scope, project_key).await?;
        let docs = self.list_memory_documents(scope, project_key).await?;
        let now = now_rfc3339();

        let indexes_dir = self.resolver.indexes_dir(scope, project_key);
        let views_dir = self.resolver.views_dir(scope, project_key);
        let state_dir = self.resolver.state_dir(scope, project_key);

        // Every artifact below is fully derived from `docs`. Stage them all, then
        // commit together (all temps written+fsync'd first, then renamed) so a
        // crash mid-refresh can't leave the index set half-updated and mutually
        // inconsistent — each file stays individually complete and the set is
        // regenerated on the next refresh (#166 item 2).
        let mut artifacts: Vec<(PathBuf, Vec<u8>)> = Vec::new();

        let lexical = LexicalIndex {
            generated_at: now.clone(),
            items: docs
                .iter()
                .map(|doc| LexicalIndexItem {
                    id: doc.frontmatter.id.clone(),
                    title: doc.frontmatter.title.clone(),
                    scope: doc.frontmatter.scope,
                    project_key: doc.frontmatter.project_key.clone(),
                    r#type: doc.frontmatter.r#type,
                    status: doc.frontmatter.status,
                    tags: doc.frontmatter.tags.clone(),
                    keywords: doc.frontmatter.retrieval.keywords.clone(),
                    entities: doc.frontmatter.retrieval.entities.clone(),
                    updated_at: doc.frontmatter.updated_at.clone(),
                    created_at: doc.frontmatter.created_at.clone(),
                    summary: derive_summary(&doc.body, 240),
                    granularity: doc.frontmatter.granularity,
                    // L1: populated at index-build time once a MemoryEmbedder backend
                    // is wired; None today → recall stays pure BM25 (inert seam).
                    embedding: None,
                })
                .collect(),
        };
        artifacts.push((
            indexes_dir.join(LEXICAL_INDEX_FILE),
            json_pretty_bytes(&lexical)?,
        ));

        let recent = RecentIndex {
            generated_at: now.clone(),
            items: docs
                .iter()
                .take(50)
                .map(|doc| RecentIndexItem {
                    id: doc.frontmatter.id.clone(),
                    title: doc.frontmatter.title.clone(),
                    updated_at: doc.frontmatter.updated_at.clone(),
                    last_accessed_at: doc.frontmatter.retrieval.last_accessed_at.clone(),
                    status: doc.frontmatter.status,
                })
                .collect(),
        };
        artifacts.push((
            indexes_dir.join(RECENT_INDEX_FILE),
            json_pretty_bytes(&recent)?,
        ));

        let graph = GraphIndex {
            generated_at: now.clone(),
            items: docs
                .iter()
                .map(|doc| GraphIndexItem {
                    id: doc.frontmatter.id.clone(),
                    related: doc.frontmatter.relations.related.clone(),
                    supersedes: doc.frontmatter.relations.supersedes.clone(),
                    contradicted_by: doc.frontmatter.relations.contradicted_by.clone(),
                })
                .collect(),
        };
        artifacts.push((
            indexes_dir.join(GRAPH_INDEX_FILE),
            json_pretty_bytes(&graph)?,
        ));

        let stale = StaleCandidatesIndex {
            generated_at: now.clone(),
            items: docs
                .iter()
                .filter(|doc| doc.frontmatter.status != DurableMemoryStatus::Active)
                .map(|doc| StaleCandidateItem {
                    id: doc.frontmatter.id.clone(),
                    title: doc.frontmatter.title.clone(),
                    status: doc.frontmatter.status,
                    updated_at: doc.frontmatter.updated_at.clone(),
                    reason: format!("status={}", doc.frontmatter.status.as_str()),
                })
                .collect(),
        };
        artifacts.push((
            indexes_dir.join(STALE_CANDIDATES_INDEX_FILE),
            json_pretty_bytes(&stale)?,
        ));

        let mut by_type = BTreeMap::new();
        let mut by_status = BTreeMap::new();
        let mut by_scope = BTreeMap::new();
        for doc in &docs {
            *by_type
                .entry(doc.frontmatter.r#type.as_str().to_string())
                .or_insert(0) += 1;
            *by_status
                .entry(doc.frontmatter.status.as_str().to_string())
                .or_insert(0) += 1;
            *by_scope
                .entry(doc.frontmatter.scope.as_str().to_string())
                .or_insert(0) += 1;
        }
        let taxonomy = TaxonomyIndex {
            generated_at: now.clone(),
            by_type,
            by_status,
            by_scope,
            total: docs.len(),
        };
        artifacts.push((
            indexes_dir.join(TAXONOMY_INDEX_FILE),
            json_pretty_bytes(&taxonomy)?,
        ));

        artifacts.push((
            views_dir.join(MEMORY_VIEW_FILE),
            build_memory_markdown_view(scope, project_key, &docs).into_bytes(),
        ));
        artifacts.push((
            views_dir.join(RECENT_VIEW_FILE),
            build_recent_markdown_view(&docs).into_bytes(),
        ));
        artifacts.push((
            views_dir.join(STALE_VIEW_FILE),
            build_stale_markdown_view(&docs).into_bytes(),
        ));
        // The dream view is user-authored after its first seed, so stage it only
        // when absent — never overwrite an existing one.
        let dream_path = views_dir.join(DREAM_VIEW_FILE);
        if !dream_path.exists() {
            artifacts.push((dream_path, build_dream_view(None).into_bytes()));
        }

        artifacts.push((
            state_dir.join("schema_version.json"),
            json_pretty_bytes(&serde_json::json!({ "version": super::MEMORY_SCHEMA_VERSION }))?,
        ));
        artifacts.push((
            state_dir.join("last_reindex.json"),
            json_pretty_bytes(&serde_json::json!({ "updated_at": now, "count": docs.len() }))?,
        ));

        crate::atomic_fs::atomic_write_batch(artifacts).await
    }

    async fn ensure_scope_dirs(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<()> {
        let project_key = self.require_project_key(scope, project_key)?;
        if scope == MemoryScope::Session {
            return Ok(());
        }
        fs::create_dir_all(self.resolver.topic_dir(scope, project_key)).await?;
        fs::create_dir_all(self.resolver.indexes_dir(scope, project_key)).await?;
        fs::create_dir_all(self.resolver.views_dir(scope, project_key)).await?;
        fs::create_dir_all(self.resolver.logs_dir(scope, project_key)).await?;
        fs::create_dir_all(self.resolver.state_dir(scope, project_key)).await?;
        Ok(())
    }

    async fn persist_session_state(&self, session_id: &str) -> io::Result<()> {
        validate_session_id(session_id)?;
        let topics = self
            .list_session_topics_without_migration(session_id)
            .await?;
        let state_path = self.resolver.session_state_path(session_id);
        let existing = if state_path.exists() {
            fs::read_to_string(&state_path)
                .await
                .ok()
                .and_then(|raw| serde_json::from_str::<SessionState>(&raw).ok())
        } else {
            None
        };
        let now = now_rfc3339();
        let mut state = existing.unwrap_or(SessionState {
            version: MEMORY_SCHEMA_VERSION,
            session_id: session_id.to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
            last_extracted_at: None,
            last_compacted_at: None,
            topics: Vec::new(),
        });
        if state.created_at.trim().is_empty() {
            state.created_at = now.clone();
        }
        state.updated_at = now;
        state.topics = topics;
        state.version = MEMORY_SCHEMA_VERSION;
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        self.write_json_file(state_path, &state).await
    }

    async fn maybe_migrate_legacy_session(&self, session_id: &str) -> io::Result<()> {
        validate_session_id(session_id)?;
        let note_dir = self.resolver.session_note_dir(session_id);
        let legacy_single = self.resolver.legacy_session_file(session_id);
        let legacy_topic_dir = self.resolver.legacy_session_topic_dir(session_id);

        if !legacy_single.exists() && !legacy_topic_dir.exists() {
            return Ok(());
        }

        fs::create_dir_all(&note_dir).await?;

        if legacy_single.exists() {
            let target = self
                .resolver
                .session_topic_path(session_id, DEFAULT_SESSION_TOPIC);
            if !target.exists() {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent).await?;
                }
                if let Err(_error) = fs::rename(&legacy_single, &target).await {
                    let content = fs::read_to_string(&legacy_single).await?;
                    crate::atomic_fs::atomic_write(&target, content.as_bytes()).await?;
                    fs::remove_file(&legacy_single).await?;
                }
            } else {
                let _ = fs::remove_file(&legacy_single).await;
            }
        }

        if legacy_topic_dir.exists() {
            let mut entries = fs::read_dir(&legacy_topic_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "md") {
                    if let Some(name) = path.file_name() {
                        let target = note_dir.join(name);
                        if !target.exists() {
                            if let Err(_error) = fs::rename(&path, &target).await {
                                let content = fs::read_to_string(&path).await?;
                                crate::atomic_fs::atomic_write(&target, content.as_bytes()).await?;
                                fs::remove_file(&path).await?;
                            }
                        } else {
                            let _ = fs::remove_file(&path).await;
                        }
                    }
                }
            }
            let _ = fs::remove_dir(&legacy_topic_dir).await;
        }

        self.persist_session_state(session_id).await?;
        Ok(())
    }

    async fn maybe_migrate_legacy_dream(&self) -> io::Result<()> {
        let view_path = self
            .resolver
            .views_dir(MemoryScope::Global, None)
            .join(DREAM_VIEW_FILE);
        if view_path.exists() {
            return Ok(());
        }
        let legacy = self
            .resolver
            .legacy_notes_root()
            .join("__dream__")
            .join("global.md");
        if !legacy.exists() {
            return Ok(());
        }
        self.ensure_scope_dirs(MemoryScope::Global, None).await?;
        let content = fs::read_to_string(&legacy).await?;
        crate::atomic_fs::atomic_write(&view_path, build_dream_view(Some(&content)).as_bytes())
            .await?;
        let _ = fs::remove_file(&legacy).await;
        Ok(())
    }

    fn session_topic_path(&self, session_id: &str, topic: &str) -> io::Result<PathBuf> {
        let session_id = validate_session_id(session_id)?;
        let topic = validate_session_topic(topic)?;
        Ok(self.resolver.session_topic_path(session_id, topic))
    }

    fn require_project_key<'a>(
        &self,
        scope: MemoryScope,
        project_key: Option<&'a str>,
    ) -> io::Result<Option<&'a str>> {
        match scope {
            MemoryScope::Global => Ok(None),
            MemoryScope::Project => project_key
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(Some)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "project scope requires project_key",
                    )
                }),
            MemoryScope::Session => Ok(project_key),
        }
    }

    async fn append_audit(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        file_name: &str,
        entry: AuditLogEntry,
    ) -> io::Result<()> {
        let path = self.resolver.logs_dir(scope, project_key).join(file_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut line = serde_json::to_string(&entry).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize audit log: {error}"),
            )
        })?;
        line.push('\n');
        // Append the single new line instead of reading + rewriting the whole
        // (append-only, unbounded) log on every mutation — the old read→push→
        // rewrite was O(history) per write and O(history²) cumulative (#235).
        use tokio::io::AsyncWriteExt;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        // fsync the appended line so a returned Ok() means it's durable —
        // `flush()` on a tokio File is a no-op, and the old atomic_write path
        // did fsync. (Matches the store's #166 durability discipline.)
        file.sync_all().await
    }

    /// Append a `{id, ts}` line per recalled `ids` to the scope's
    /// `access_log.jsonl` (#264) — the cheap access signal `memory_value` folds
    /// into capacity-eviction scoring via [`access_log::access_multiplier`],
    /// standing in for writing `last_accessed_at` back onto every recalled
    /// document (which would force a full per-scope index rebuild per recall).
    ///
    /// Best-effort and O(ids): one file open, one buffered write, no fsync (this
    /// is a soft signal, not an audit trail — losing a line just makes the access
    /// signal for that memory slightly stale, whereas blocking or failing recall
    /// on a log write is never acceptable). Any IO error is logged and swallowed;
    /// callers on the recall path must be able to call this unconditionally.
    async fn record_memory_accesses(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        ids: &[String],
    ) {
        if ids.is_empty() {
            return;
        }
        let path = self
            .resolver
            .logs_dir(scope, project_key)
            .join(ACCESS_LOG_FILE);
        if let Err(error) = self.record_memory_accesses_inner(&path, ids).await {
            tracing::debug!(
                target: "bamboo_memory::access_log",
                error = %error,
                path = %path.display(),
                "failed to append access log (best-effort; recall is unaffected)"
            );
        }
    }

    async fn record_memory_accesses_inner(&self, path: &Path, ids: &[String]) -> io::Result<()> {
        // Guards the append + size-check + (rare) compaction as one critical
        // section, scoped to just this scope's access-log file — NOT the
        // scope-wide `scope_lock`, so a burst of concurrent recalls never
        // contends with concurrent writers mutating the scope's memory docs.
        let lock = self.path_lock(path.to_path_buf());
        let _guard = lock.lock().await;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let ts = now_rfc3339();
        let mut buf = String::new();
        for id in ids {
            let line = serde_json::to_string(&AccessLogEntry {
                id: id.clone(),
                ts: ts.clone(),
            })
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to serialize access log entry: {error}"),
                )
            })?;
            buf.push_str(&line);
            buf.push('\n');
        }

        use tokio::io::AsyncWriteExt;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        file.write_all(buf.as_bytes()).await?;
        file.flush().await?;
        let size = file.metadata().await.map(|meta| meta.len()).unwrap_or(0);
        drop(file);

        if size > ACCESS_LOG_COMPACT_TRIGGER_BYTES {
            self.compact_access_log_at(path).await?;
        }
        Ok(())
    }

    /// Rewrite the access log at `path` keeping only the entries
    /// [`access_log::compact_entries`] retains — bounded size, safe under
    /// concurrent recalls because the caller already holds `path`'s lock.
    /// Not a hot-path operation: only runs when a scope's log crosses
    /// [`ACCESS_LOG_COMPACT_TRIGGER_BYTES`], which — given the bound
    /// `compact_entries` itself enforces — happens on the order of once per
    /// several thousand recalls to that scope, not once per recall.
    async fn compact_access_log_at(&self, path: &Path) -> io::Result<()> {
        let raw = match fs::read_to_string(path).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let entries = access_log::parse_access_log(&raw);
        let compacted = access_log::compact_entries(entries, chrono::Utc::now());
        let mut out = String::with_capacity(raw.len());
        for entry in &compacted {
            let line = serde_json::to_string(entry).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to serialize access log entry: {error}"),
                )
            })?;
            out.push_str(&line);
            out.push('\n');
        }
        crate::atomic_fs::atomic_write(path, out.as_bytes()).await
    }

    /// Lazily aggregate a scope's `access_log.jsonl` into per-id [`AccessStats`]
    /// — called only from the (infrequent) capacity gardener pass, never from
    /// the recall path. Best-effort: any read/parse failure (including "file
    /// doesn't exist yet", the common case for a scope with no recall history
    /// since #264 shipped) degrades to an empty map rather than failing the
    /// caller, which via [`access_log::access_multiplier`] means every doc gets
    /// the neutral `1.0` multiplier — i.e. scoring identical to before #264.
    async fn access_log_stats(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> HashMap<String, AccessStats> {
        let path = self
            .resolver
            .logs_dir(scope, project_key)
            .join(ACCESS_LOG_FILE);
        match self.read_access_log_stats_at(&path).await {
            Ok(stats) => stats,
            Err(error) => {
                tracing::debug!(
                    target: "bamboo_memory::access_log",
                    error = %error,
                    path = %path.display(),
                    "failed to read access log (best-effort; scoring falls back to neutral)"
                );
                HashMap::new()
            }
        }
    }

    async fn read_access_log_stats_at(
        &self,
        path: &Path,
    ) -> io::Result<HashMap<String, AccessStats>> {
        let lock = self.path_lock(path.to_path_buf());
        let _guard = lock.lock().await;
        let raw = match fs::read_to_string(path).await {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(error) => return Err(error),
        };
        let entries = access_log::parse_access_log(&raw);
        Ok(access_log::aggregate_access_stats(
            &entries,
            chrono::Utc::now(),
        ))
    }

    async fn write_json_file<T: serde::Serialize>(
        &self,
        path: PathBuf,
        value: &T,
    ) -> io::Result<()> {
        let data = json_pretty_bytes(value)?;
        crate::atomic_fs::atomic_write(&path, &data).await
    }

    async fn read_optional_trimmed_text_file(&self, path: PathBuf) -> io::Result<Option<String>> {
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path).await?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    }

    async fn read_optional_json_file<T: serde::de::DeserializeOwned>(
        &self,
        path: PathBuf,
    ) -> io::Result<Option<T>> {
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path).await?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        serde_json::from_str(trimmed).map(Some).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to deserialize json: {error}"),
            )
        })
    }

    async fn read_scope_dream_view(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
    ) -> io::Result<Option<String>> {
        let project_key = self.require_project_key(scope, project_key)?;
        if scope == MemoryScope::Global {
            self.maybe_migrate_legacy_dream().await?;
        }
        let path = self
            .resolver
            .views_dir(scope, project_key)
            .join(DREAM_VIEW_FILE);
        self.read_optional_trimmed_text_file(path).await
    }

    async fn write_scope_dream_view(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        content: &str,
    ) -> io::Result<PathBuf> {
        let project_key = self.require_project_key(scope, project_key)?;
        let lock = self.scope_lock(scope, project_key);
        let _guard = lock.lock().await;
        self.ensure_scope_dirs(scope, project_key).await?;
        let path = self
            .resolver
            .views_dir(scope, project_key)
            .join(DREAM_VIEW_FILE);
        crate::atomic_fs::atomic_write(&path, build_dream_view(Some(content)).as_bytes()).await?;
        self.write_state_marker(
            scope,
            project_key,
            "last_dream.json",
            json_obj("updated_at", now_rfc3339()),
        )
        .await?;
        Ok(path)
    }

    async fn write_state_marker(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        file_name: &str,
        value: serde_json::Value,
    ) -> io::Result<()> {
        self.write_json_file(
            self.resolver.state_dir(scope, project_key).join(file_name),
            &value,
        )
        .await
    }
}

fn json_obj(key: &str, value: String) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(key.to_string(), serde_json::Value::String(value));
    serde_json::Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn session_topics_roundtrip_and_migrate_legacy_notes() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let legacy_dir = dir.path().join("notes");
        fs::create_dir_all(&legacy_dir).await.unwrap();
        fs::write(legacy_dir.join("session-1.md"), "legacy note")
            .await
            .unwrap();

        let content = store
            .read_session_topic("session-1", DEFAULT_SESSION_TOPIC)
            .await
            .unwrap();
        assert_eq!(content.as_deref(), Some("legacy note"));

        store
            .append_session_topic("session-1", "backend", "API finalized")
            .await
            .unwrap();
        let topics = store.list_session_topics("session-1").await.unwrap();
        assert_eq!(topics, vec!["backend", "default"]);
    }

    #[tokio::test]
    async fn split_memory_creates_atomic_children_and_supersedes_source() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let blob = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "mixed blob",
                "fact A\n\n---\n\nfact B",
                &[],
                Some("s1"),
                "test",
                false,
                None,
            )
            .await
            .unwrap();

        let pieces = vec![
            MemorySplitPiece {
                title: "fact A".to_string(),
                r#type: None,
                content: "fact A".to_string(),
                tags: vec!["a".to_string()],
            },
            MemorySplitPiece {
                title: "fact B".to_string(),
                r#type: Some(DurableMemoryType::Reference),
                content: "fact B".to_string(),
                tags: vec![],
            },
        ];

        let result = store
            .split_memory(&blob.frontmatter.id, None, &pieces, Some("s1"), "test")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.new_ids.len(), 2);
        assert_eq!(result.source_id, blob.frontmatter.id);

        let source = store
            .get_memory(&blob.frontmatter.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(source.frontmatter.status, DurableMemoryStatus::Superseded);

        for new_id in &result.new_ids {
            let child = store.get_memory(new_id, None).await.unwrap().unwrap();
            assert_eq!(child.frontmatter.status, DurableMemoryStatus::Active);
            assert!(child
                .frontmatter
                .relations
                .supersedes
                .contains(&blob.frontmatter.id));
        }
    }

    #[tokio::test]
    async fn scan_blob_candidates_flags_merged_docs_worst_first() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        // Atomic doc: 0 appended sections — must NOT be flagged.
        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "atomic",
                "single fact",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();

        // Simulate a blob via explicit merges (each appends one `---` section).
        let blob = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "blobby",
                "fact one",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();
        for extra in ["fact two", "fact three", "fact four"] {
            store
                .merge_memory(&blob.frontmatter.id, None, extra, &[], Some("s"), "t", &[])
                .await
                .unwrap();
        }

        let report = store
            .scan_blob_candidates(MemoryScope::Global, None, 3, 20)
            .await
            .unwrap();

        assert_eq!(report.scanned, 2);
        assert_eq!(report.flagged, 1);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0].title, "blobby");
        assert_eq!(report.items[0].appended_sections, 3);
    }

    #[tokio::test]
    async fn find_duplicate_candidates_ranks_overlap_and_skips_unrelated() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Release freeze rule",
                "Mobile release freeze begins Tuesday for the release cut.",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "Unrelated cat fact",
                "The quiet cat napped under the warm windowsill.",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();

        let candidates = store
            .find_duplicate_candidates(
                MemoryScope::Global,
                None,
                None,
                "Release freeze",
                "Mobile release freeze begins Tuesday",
                &[],
                5,
            )
            .await
            .unwrap();

        assert!(!candidates.is_empty());
        assert_eq!(candidates[0].title, "Release freeze rule");
        assert!(candidates[0].score > 0.0);
        // The unrelated memory shares no keywords and must be filtered out.
        assert!(!candidates.iter().any(|c| c.title == "Unrelated cat fact"));
    }

    #[tokio::test]
    async fn split_memory_rejects_oversized_piece_without_partial_write() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let blob = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "blob",
                "seed",
                &[],
                Some("s1"),
                "test",
                false,
                None,
            )
            .await
            .unwrap();

        let big = "y".repeat(MAX_DURABLE_MEMORY_BODY_CHARS + 1);
        let pieces = vec![
            MemorySplitPiece {
                title: "ok piece".to_string(),
                r#type: None,
                content: "small".to_string(),
                tags: vec![],
            },
            MemorySplitPiece {
                title: "too big".to_string(),
                r#type: None,
                content: big,
                tags: vec![],
            },
        ];

        assert!(store
            .split_memory(&blob.frontmatter.id, None, &pieces, Some("s1"), "test")
            .await
            .is_err());

        // Up-front validation means nothing was written and the source is untouched.
        let source = store
            .get_memory(&blob.frontmatter.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(source.frontmatter.status, DurableMemoryStatus::Active);
    }

    #[tokio::test]
    async fn explicit_merge_over_cap_fails_loudly_without_appending() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let near_cap = "x".repeat(MAX_DURABLE_MEMORY_BODY_CHARS - 20);
        let existing = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "near cap memory",
                &near_cap,
                &[],
                Some("s1"),
                "test",
                false,
                None,
            )
            .await
            .unwrap();

        // Appending anything non-trivial would push the body past the cap.
        let result = store
            .merge_memory(
                &existing.frontmatter.id,
                None,
                "a second unrelated fact that does not fit",
                &[],
                Some("s1"),
                "test",
                &[],
            )
            .await;
        assert!(result.is_err());

        // The body must be exactly what it was — no partial append.
        let after = store
            .get_memory(&existing.frontmatter.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.body, near_cap);
    }

    #[tokio::test]
    async fn auto_merge_over_cap_creates_new_atomic_memory_instead_of_appending() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let near_cap = "x".repeat(MAX_DURABLE_MEMORY_BODY_CHARS - 20);
        let first = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "shared title",
                &near_cap,
                &[],
                Some("s1"),
                "test",
                false,
                None,
            )
            .await
            .unwrap();

        // Same title/type → would normally auto-merge, but the append would exceed
        // the cap, so the structural guard must fall through to a NEW memory.
        let second = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "shared title",
                "a distinct second fact",
                &[],
                Some("s1"),
                "test",
                true,
                None,
            )
            .await
            .unwrap();

        assert_ne!(
            second.frontmatter.id, first.frontmatter.id,
            "over-cap auto-merge must create a separate memory, not append"
        );
        // The original body is untouched (no `---` accretion).
        let original = store
            .get_memory(&first.frontmatter.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.body, near_cap);
    }

    #[tokio::test]
    async fn scan_duplicate_clusters_groups_near_duplicates_and_skips_unrelated() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        for (title, content) in [
            (
                "Release freeze rule",
                "Mobile release freeze begins Tuesday for the release cut.",
            ),
            (
                "Release freeze timing",
                "Mobile release freeze begins Tuesday for the cut.",
            ),
            (
                "Unrelated cat fact",
                "The quiet cat napped under the warm windowsill.",
            ),
        ] {
            store
                .write_memory(
                    MemoryScope::Global,
                    None,
                    DurableMemoryType::Project,
                    title,
                    content,
                    &[],
                    Some("s"),
                    "t",
                    false,
                    None,
                )
                .await
                .unwrap();
        }

        let report = store
            .scan_duplicate_clusters(MemoryScope::Global, None, 0.3, 5, 20)
            .await
            .unwrap();

        assert_eq!(report.scanned, 3);
        assert_eq!(report.clusters.len(), 1);
        let cluster = &report.clusters[0];
        assert_eq!(cluster.members.len(), 2);
        assert!(cluster.max_score > 0.0);
        // The unrelated cat memory must not be clustered with the freeze memories.
        assert!(!cluster
            .members
            .iter()
            .any(|m| m.title == "Unrelated cat fact"));
    }

    #[tokio::test]
    async fn consolidate_memories_creates_canonical_and_supersedes_sources() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let first = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "freeze v1",
                "Mobile release freeze begins Tuesday.",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();
        let second = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Project,
                "freeze v2",
                "Release freeze starts Tuesday for the cut.",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();

        let merged = MemorySplitPiece {
            title: "Mobile release freeze is Tuesday".to_string(),
            r#type: Some(DurableMemoryType::Project),
            content: "Mobile release freeze begins Tuesday for the release cut.".to_string(),
            tags: vec!["release".to_string()],
        };
        let ids = vec![first.frontmatter.id.clone(), second.frontmatter.id.clone()];
        let result = store
            .consolidate_memories(&ids, None, &merged, Some("s"), "test")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.superseded_ids.len(), 2);
        // The canonical memory is active, supersedes both sources, and carries the
        // merged body.
        let canonical = store
            .get_memory(&result.new_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(canonical.frontmatter.status, DurableMemoryStatus::Active);
        assert_eq!(canonical.body, merged.content);
        assert!(canonical
            .frontmatter
            .relations
            .supersedes
            .contains(&first.frontmatter.id));
        assert!(canonical
            .frontmatter
            .relations
            .supersedes
            .contains(&second.frontmatter.id));
        // Both sources are superseded.
        for id in &ids {
            let source = store.get_memory(id, None).await.unwrap().unwrap();
            assert_eq!(source.frontmatter.status, DurableMemoryStatus::Superseded);
        }
    }

    #[tokio::test]
    async fn consolidate_memories_rejects_single_source_and_missing_source() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let only = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::User,
                "lonely fact",
                "just one fact",
                &[],
                Some("s"),
                "t",
                false,
                None,
            )
            .await
            .unwrap();
        let merged = MemorySplitPiece {
            title: "merged".to_string(),
            r#type: None,
            content: "merged body".to_string(),
            tags: vec![],
        };

        // Fewer than two sources is an error.
        assert!(store
            .consolidate_memories(
                &[only.frontmatter.id.clone()],
                None,
                &merged,
                Some("s"),
                "t"
            )
            .await
            .is_err());

        // A missing source aborts (Ok(None)) without superseding the real one.
        let ids = vec![
            only.frontmatter.id.clone(),
            "mem_does_not_exist".to_string(),
        ];
        assert!(store
            .consolidate_memories(&ids, None, &merged, Some("s"), "t")
            .await
            .unwrap()
            .is_none());
        let untouched = store
            .get_memory(&only.frontmatter.id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(untouched.frontmatter.status, DurableMemoryStatus::Active);
    }

    #[tokio::test]
    async fn durable_write_query_and_get_work() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let doc = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze begins next week",
                "Merge freeze begins on Tuesday for mobile release cut.",
                &["release".to_string(), "freeze".to_string()],
                Some("session-1"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        let result = store
            .query_scope(
                MemoryScope::Project,
                Some("proj-1"),
                Some("release freeze"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(result.matched_count, 1);
        assert_eq!(result.items[0].id, doc.frontmatter.id);

        let fetched = store
            .get_memory(&doc.frontmatter.id, Some("proj-1"))
            .await
            .unwrap()
            .expect("memory exists");
        assert!(fetched.body.contains("Merge freeze"));
    }

    /// L2: a near-identical RESTATEMENT (different title wording, ~same body)
    /// auto-merges into the existing memory. This is the only write-time merge —
    /// it's the case where accreting is unambiguously not lossy.
    #[tokio::test]
    async fn write_memory_merges_a_near_identical_restatement() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let original = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Prod deploy uses blue-green with a 10 minute soak",
                "Production deploys use a blue-green strategy with a ten minute soak window.",
                &["deploy".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let merged = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Prod deploy uses blue-green with a 10 minute soak window",
                "Production deploy uses blue-green with a 10 minute soak before cutover.",
                &["deploy".to_string()],
                Some("session-2"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            merged.frontmatter.id, original.frontmatter.id,
            "a near-identical restatement should merge into the existing memory"
        );
        assert!(merged
            .body
            .contains("blue-green strategy with a ten minute soak window"));
        assert!(merged
            .body
            .contains("blue-green with a 10 minute soak before cutover"));

        let docs = store
            .list_memory_documents(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap();
        assert_eq!(docs.len(), 1, "restatement merges into one durable memory");
    }

    /// L2: a same-topic but REWORDED write (a probable-but-not-certain dup) does
    /// NOT auto-merge — it stays a separate atomic memory, LINKED to the near-dup.
    /// Merging is lossy and irreversible, so at write time we prefer a cheap,
    /// reversible link and leave any real consolidation to L3's model-gated pass.
    #[tokio::test]
    async fn write_memory_relates_similar_but_distinct_memory_instead_of_merging() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let original = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze begins next week",
                "Merge freeze begins on Tuesday for the mobile release cut.",
                &["release".to_string(), "freeze".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let second = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Mobile release freeze starts Tuesday",
                "Stakeholders confirmed the mobile release freeze starts Tuesday.",
                &["mobile".to_string(), "release".to_string()],
                Some("session-2"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        assert_ne!(
            second.frontmatter.id, original.frontmatter.id,
            "a reworded near-dup must NOT force-merge"
        );
        assert!(
            second
                .frontmatter
                .relations
                .related
                .contains(&original.frontmatter.id),
            "the new memory should link to the near-dup, got {:?}",
            second.frontmatter.relations.related
        );

        let docs = store
            .list_memory_documents(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap();
        assert_eq!(docs.len(), 2, "reworded near-dup stays a separate memory");
    }

    /// L2 precision: two memories that share only a COMMON word (low IDF) are
    /// neither merged nor linked — the old raw count-overlap gate could have
    /// force-merged them. This is the corrupting false-merge the redesign kills.
    #[tokio::test]
    async fn write_memory_does_not_link_on_common_words_alone() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let first = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Kafka consumer lag alerting thresholds",
                "Kafka consumer lag alerts page the team when we deploy a bad build.",
                &["kafka".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let second = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Postgres autovacuum tuning knobs",
                "Postgres autovacuum thresholds are tuned per table before we deploy.",
                &["postgres".to_string()],
                Some("session-2"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        assert_ne!(second.frontmatter.id, first.frontmatter.id);
        assert!(
            second.frontmatter.relations.related.is_empty(),
            "docs sharing only a common word must not be linked, got {:?}",
            second.frontmatter.relations.related
        );

        let docs = store
            .list_memory_documents(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap();
        assert_eq!(docs.len(), 2, "unrelated writes stay separate");
    }

    /// L2 precision: the SAME content recorded verbatim under a very different
    /// title is an unambiguous dup — it merges (idempotently) rather than
    /// accumulating a second copy, even though the titles score far apart.
    #[tokio::test]
    async fn write_memory_dedups_exact_body_under_a_different_title() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let body = "The staging cluster autoscaler caps at 12 nodes during business hours.";

        let original = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Staging autoscaler node cap",
                body,
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let again = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Totally unrelated sounding title about capacity limits somewhere",
                body,
                &[],
                Some("session-2"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            again.frontmatter.id, original.frontmatter.id,
            "verbatim duplicate content should merge idempotently"
        );
        let docs = store
            .list_memory_documents(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap();
        assert_eq!(
            docs.len(),
            1,
            "verbatim dup must not create a second memory"
        );
        // Idempotent: the body wasn't doubled.
        assert_eq!(docs[0].body.matches(body).count(), 1);
    }

    /// L4: `count_all_memories` sums topic files across global + project scopes
    /// cheaply, so the volume-triggered maintenance pass can detect growth.
    #[tokio::test]
    async fn count_all_memories_sums_across_scopes() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        assert_eq!(store.count_all_memories().await.unwrap(), 0);

        let write = |scope, project_key: Option<&'static str>, title: &'static str| {
            let store = &store;
            async move {
                store
                    .write_memory(
                        scope,
                        project_key,
                        if matches!(scope, MemoryScope::Global) {
                            DurableMemoryType::Reference
                        } else {
                            DurableMemoryType::Project
                        },
                        title,
                        "body content for the memory",
                        &[],
                        Some("s"),
                        "m",
                        false,
                        None,
                    )
                    .await
                    .unwrap();
            }
        };

        write(MemoryScope::Global, None, "Global fact one").await;
        write(MemoryScope::Global, None, "Global fact two").await;
        write(MemoryScope::Project, Some("proj-a"), "Project A fact").await;
        write(MemoryScope::Project, Some("proj-b"), "Project B fact").await;

        assert_eq!(
            store
                .count_scope_memories(MemoryScope::Global, None)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .count_scope_memories(MemoryScope::Project, Some("proj-a"))
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.count_all_memories().await.unwrap(), 4);
    }

    /// L5: capacity eviction archives the overflow OUT of recall (never deletes),
    /// exempts non-Project types, and respects the per-run cap.
    #[tokio::test]
    async fn enforce_scope_capacity_archives_overflow_exempts_and_never_deletes() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");

        let write = |title: &'static str, r#type| {
            let store = &store;
            async move {
                store
                    .write_memory(
                        MemoryScope::Project,
                        pk,
                        r#type,
                        title,
                        "body content for the memory",
                        &[],
                        Some("s"),
                        "m",
                        false,
                        None,
                    )
                    .await
                    .unwrap();
            }
        };

        for i in 0..6 {
            write(
                match i {
                    0 => "Project fact zero",
                    1 => "Project fact one",
                    2 => "Project fact two",
                    3 => "Project fact three",
                    4 => "Project fact four",
                    _ => "Project fact five",
                },
                DurableMemoryType::Project,
            )
            .await;
        }
        // A Reference memory in the same scope — high-value, must be EXEMPT.
        write("Curated reference doc", DurableMemoryType::Reference).await;

        let total_before = store
            .count_scope_memories(MemoryScope::Project, pk)
            .await
            .unwrap();
        assert_eq!(total_before, 7);

        // capacity=0 is a no-op.
        assert!(store
            .enforce_scope_capacity(MemoryScope::Project, pk, 0, 100)
            .await
            .unwrap()
            .is_empty());

        // Per-run cap: overflow is 7-3=4, but max_archivals=2 → only 2 this run.
        let first = store
            .enforce_scope_capacity(MemoryScope::Project, pk, 3, 2)
            .await
            .unwrap();
        assert_eq!(first.len(), 2, "per-run cap bounds archivals");

        // Next run drains the rest down to capacity.
        let second = store
            .enforce_scope_capacity(MemoryScope::Project, pk, 3, 100)
            .await
            .unwrap();
        assert_eq!(second.len(), 2, "remaining overflow archived");

        let docs = store
            .list_memory_documents(MemoryScope::Project, pk)
            .await
            .unwrap();
        // Never deleted: all 7 files still present.
        assert_eq!(docs.len(), 7, "archive must not delete");
        let scorable = docs
            .iter()
            .filter(|d| {
                matches!(
                    d.frontmatter.status,
                    DurableMemoryStatus::Active | DurableMemoryStatus::Stale
                )
            })
            .count();
        assert_eq!(scorable, 3, "recallable count bounded to capacity");
        // The Reference doc is exempt: still Active.
        let reference = docs
            .iter()
            .find(|d| d.frontmatter.r#type == DurableMemoryType::Reference)
            .unwrap();
        assert_eq!(reference.frontmatter.status, DurableMemoryStatus::Active);
        // Everything archived is a Project memory.
        for id in first.iter().chain(second.iter()) {
            let doc = docs.iter().find(|d| &d.frontmatter.id == id).unwrap();
            assert_eq!(doc.frontmatter.r#type, DurableMemoryType::Project);
            assert_eq!(doc.frontmatter.status, DurableMemoryStatus::Archived);
        }
    }

    /// L5 guard: when exempt (Reference/User/Feedback) memories alone exceed the
    /// capacity, the bound is unachievable by archiving only Project memories — so
    /// the pass archives NOTHING rather than stripping every Project memory.
    #[tokio::test]
    async fn enforce_scope_capacity_skips_when_capacity_below_exempt_floor() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");
        let write = |title: &'static str, r#type| {
            let store = &store;
            async move {
                store
                    .write_memory(
                        MemoryScope::Project,
                        pk,
                        r#type,
                        title,
                        "body content for the memory",
                        &[],
                        Some("s"),
                        "m",
                        false,
                        None,
                    )
                    .await
                    .unwrap();
            }
        };
        // 3 exempt (Reference) + 2 evictable (Project).
        write("Ref one", DurableMemoryType::Reference).await;
        write("Ref two", DurableMemoryType::Reference).await;
        write("Ref three", DurableMemoryType::Reference).await;
        write("Proj one", DurableMemoryType::Project).await;
        write("Proj two", DurableMemoryType::Project).await;

        // capacity=2 < 3 exempt → unachievable → archive nothing (don't strip Project).
        let archived = store
            .enforce_scope_capacity(MemoryScope::Project, pk, 2, 100)
            .await
            .unwrap();
        assert!(
            archived.is_empty(),
            "must not strip Project memories chasing an unreachable bound"
        );
        let docs = store
            .list_memory_documents(MemoryScope::Project, pk)
            .await
            .unwrap();
        assert!(docs
            .iter()
            .all(|d| d.frontmatter.status == DurableMemoryStatus::Active));
    }

    /// #264: a successful `query_scope` recall appends a `{id, ts}` line per
    /// returned item to the scope's `access_log.jsonl` — the cheap signal that
    /// stands in for writing `last_accessed_at` back onto the document.
    #[tokio::test]
    async fn query_scope_appends_access_log_for_returned_items() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");

        let doc = store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "Release freeze begins next week",
                "Merge freeze begins on Tuesday for mobile release cut.",
                &[],
                Some("session-1"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        let log_path = store
            .resolver()
            .logs_dir(MemoryScope::Project, pk)
            .join(ACCESS_LOG_FILE);
        assert!(!log_path.exists(), "no access log before any recall");

        let result = store
            .query_scope(
                MemoryScope::Project,
                pk,
                Some("release freeze"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.items.len(), 1);

        let raw = fs::read_to_string(&log_path).await.unwrap();
        let entries = access_log::parse_access_log(&raw);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, doc.frontmatter.id);
        assert!(parse_rfc3339(&entries[0].ts).is_some());

        // A second recall appends a second line rather than rewriting the file.
        store
            .query_scope(
                MemoryScope::Project,
                pk,
                Some("release freeze"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        let raw = fs::read_to_string(&log_path).await.unwrap();
        assert_eq!(access_log::parse_access_log(&raw).len(), 2);
    }

    /// #264: recall must never fail because the (best-effort) access log couldn't
    /// be written. Simulate a write failure by making the log's path itself a
    /// directory, so `OpenOptions::append().open()` on it errors.
    #[tokio::test]
    async fn recall_succeeds_even_if_access_log_write_fails() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");

        let doc = store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "Deploy pipeline rewritten",
                "The deploy pipeline was rewritten to use the new build cache.",
                &[],
                Some("session-1"),
                "main-model",
                true,
                None,
            )
            .await
            .unwrap();

        let log_path = store
            .resolver()
            .logs_dir(MemoryScope::Project, pk)
            .join(ACCESS_LOG_FILE);
        // Force the append to fail: the log "file" path is actually a directory.
        fs::create_dir_all(&log_path).await.unwrap();

        let result = store
            .query_scope(
                MemoryScope::Project,
                pk,
                Some("deploy pipeline"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .expect("recall must succeed despite the access-log write failing");
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id, doc.frontmatter.id);
    }

    /// #264: with no access-log data at all (no file written), the multiplier is
    /// neutral for every id — the exact back-compat guarantee `memory_value`'s
    /// doc comment promises.
    #[tokio::test]
    async fn access_log_stats_defaults_to_empty_without_a_log_file() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let stats = store
            .access_log_stats(MemoryScope::Project, Some("proj-1"))
            .await;
        assert!(stats.is_empty());
        assert_eq!(access_log::access_multiplier(stats.get("anything")), 1.0);
    }

    /// L5 (#264): the access-frequency multiplier only ever helps a memory
    /// survive capacity eviction. Two Project memories written back-to-back are
    /// otherwise identical (same confidence, same status, same-day recency), so
    /// with capacity forcing exactly one archival, the one with recall history
    /// must be the one spared.
    #[tokio::test]
    async fn accessed_memory_outscores_unaccessed_peer_in_capacity_eviction() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");

        let accessed = store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "Frequently recalled fact",
                "This fact keeps getting pulled into context.",
                &[],
                Some("s"),
                "m",
                false,
                None,
            )
            .await
            .unwrap();
        let unaccessed = store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "Never recalled fact",
                "This fact has never been looked at since it was written.",
                &[],
                Some("s"),
                "m",
                false,
                None,
            )
            .await
            .unwrap();

        // Simulate recall history for `accessed` only, several times over.
        let ids = vec![accessed.frontmatter.id.clone()];
        for _ in 0..5 {
            store
                .record_memory_accesses(MemoryScope::Project, pk, &ids)
                .await;
        }

        // capacity=1 with 2 scorable Project docs forces exactly one archival.
        let archived = store
            .enforce_scope_capacity(MemoryScope::Project, pk, 1, 10)
            .await
            .unwrap();
        assert_eq!(archived, vec![unaccessed.frontmatter.id.clone()]);

        let survivor = store
            .get_memory(&accessed.frontmatter.id, pk)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(survivor.frontmatter.status, DurableMemoryStatus::Active);
    }

    /// #264: the log auto-compacts once it crosses the byte trigger, so it stays
    /// bounded instead of growing forever. Seed it with many stale (aged-out)
    /// entries past the compaction trigger size, then perform one more recall
    /// access — which appends, checks the size, and (because the file is now
    /// over the trigger) rewrites it via `compact_entries`, dropping everything
    /// outside the aging window.
    #[tokio::test]
    async fn access_log_auto_compacts_when_over_byte_threshold() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");
        let log_path = store
            .resolver()
            .logs_dir(MemoryScope::Project, pk)
            .join(ACCESS_LOG_FILE);
        fs::create_dir_all(log_path.parent().unwrap())
            .await
            .unwrap();

        // Well past both the 90-day aging window and the byte trigger.
        let stale_ts = (chrono::Utc::now() - chrono::Duration::days(365)).to_rfc3339();
        let mut raw = String::new();
        for i in 0..6000 {
            raw.push_str(
                &serde_json::to_string(&AccessLogEntry {
                    id: format!("stale-mem-{i}"),
                    ts: stale_ts.clone(),
                })
                .unwrap(),
            );
            raw.push('\n');
        }
        assert!(
            raw.len() as u64 > ACCESS_LOG_COMPACT_TRIGGER_BYTES,
            "fixture must actually exceed the compaction trigger"
        );
        fs::write(&log_path, raw.as_bytes()).await.unwrap();

        // One more access — triggers the size check + compaction inside the same
        // best-effort append call.
        store
            .record_memory_accesses(MemoryScope::Project, pk, &["fresh-mem".to_string()])
            .await;

        let compacted_raw = fs::read_to_string(&log_path).await.unwrap();
        assert!(
            (compacted_raw.len() as u64) < ACCESS_LOG_COMPACT_TRIGGER_BYTES,
            "compaction must bound the file back under the trigger size, got {} bytes",
            compacted_raw.len()
        );
        let entries = access_log::parse_access_log(&compacted_raw);
        // Every stale entry aged out; only the fresh one survives.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "fresh-mem");
    }

    /// Many `MemoryStore` instances pointed at the SAME data dir (the real
    /// concurrency pattern — one store per handler / gardener pass inside one
    /// process) write distinct memories to the same scope at once. The per-scope
    /// lock must serialize the read-modify-write + index refresh so every document
    /// is persisted and the lexical index — fully rebuilt on each refresh — ends up
    /// listing all of them. Without the lock, a refresh that reads the doc set
    /// mid-write would drop entries (lost updates / inconsistent index).
    #[tokio::test]
    async fn concurrent_writes_to_same_scope_persist_all_and_keep_index_consistent() {
        let dir = tempdir().unwrap();
        const WRITERS: usize = 16;

        let mut handles = Vec::with_capacity(WRITERS);
        for i in 0..WRITERS {
            // A fresh store per task on purpose: a per-instance lock would not help
            // here, only the process-global registry does.
            let store = MemoryStore::new(dir.path());
            handles.push(tokio::spawn(async move {
                store
                    .write_memory(
                        MemoryScope::Project,
                        Some("proj-concurrency"),
                        DurableMemoryType::Project,
                        &format!("Concurrent fact number {i}"),
                        &format!("Distinct atomic content for writer {i}."),
                        &[format!("writer-{i}")],
                        Some("session-conc"),
                        "main-model",
                        // No merging: each write must create its own document.
                        false,
                        None,
                    )
                    .await
                    .expect("concurrent write succeeds")
                    .frontmatter
                    .id
            }));
        }

        let mut written_ids = HashSet::new();
        for handle in handles {
            written_ids.insert(handle.await.expect("writer task joins"));
        }
        assert_eq!(
            written_ids.len(),
            WRITERS,
            "every concurrent write allocated a unique id (no collisions)"
        );

        // All N documents are on disk.
        let store = MemoryStore::new(dir.path());
        let docs = store
            .list_memory_documents(MemoryScope::Project, Some("proj-concurrency"))
            .await
            .unwrap();
        let doc_ids: HashSet<String> = docs.iter().map(|d| d.frontmatter.id.clone()).collect();
        assert_eq!(doc_ids, written_ids, "all written documents are persisted");

        // The lexical index (rebuilt wholesale on every refresh) lists all N — i.e.
        // no concurrent refresh observed a partial doc set and clobbered the index.
        let lexical = store
            .read_lexical_index(MemoryScope::Project, Some("proj-concurrency"))
            .await
            .unwrap()
            .expect("lexical index exists after writes");
        let indexed_ids: HashSet<String> =
            lexical.items.iter().map(|item| item.id.clone()).collect();
        assert_eq!(
            indexed_ids, written_ids,
            "lexical index is consistent with the full document set under concurrency"
        );
    }

    /// Directly exercises mutual exclusion: tasks sharing the process-global lock
    /// for a scope must never overlap inside the guarded section. A shared counter
    /// incremented on enter / decremented on exit can only ever read 1 while locked
    /// if the lock truly serializes; any interleave would push it to 2+.
    #[tokio::test]
    async fn scope_lock_serializes_critical_sections() {
        let dir = tempdir().unwrap();
        let in_section = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..32 {
            let store = MemoryStore::new(dir.path());
            let in_section = Arc::clone(&in_section);
            let max_seen = Arc::clone(&max_seen);
            handles.push(tokio::spawn(async move {
                let lock = store.scope_lock(MemoryScope::Global, None);
                let _guard = lock.lock().await;
                let now = in_section.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                // Yield to give any racing task a chance to observe overlap if the
                // lock were not actually held across the await point.
                tokio::task::yield_now().await;
                in_section.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(
            max_seen.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "at most one task is ever inside a scope-locked critical section"
        );
    }

    #[tokio::test]
    async fn merge_memory_updates_target_and_supersedes_sources() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let target = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze begins next week",
                "Merge freeze begins on Tuesday.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        let source = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Mobile release note",
                "Stakeholders confirmed freeze applies to mobile release cut.",
                &["mobile".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let merge = store
            .merge_memory(
                &target.frontmatter.id,
                Some("proj-1"),
                "Additional confirmation from a later session.",
                &["confirmed".to_string()],
                Some("session-2"),
                "main-model",
                std::slice::from_ref(&source.frontmatter.id),
            )
            .await
            .unwrap()
            .expect("merge target exists");

        assert!(merge.changed);
        assert!(merge.appended);
        assert!(merge.tags_updated);
        assert_eq!(merge.superseded_ids, vec![source.frontmatter.id.clone()]);

        let merged_doc = store
            .get_memory(&target.frontmatter.id, Some("proj-1"))
            .await
            .unwrap()
            .expect("merged memory exists");
        assert!(merged_doc
            .body
            .contains("Additional confirmation from a later session."));
        assert!(merged_doc
            .frontmatter
            .tags
            .contains(&"confirmed".to_string()));
        assert!(merged_doc
            .frontmatter
            .relations
            .supersedes
            .contains(&source.frontmatter.id));

        let source_doc = store
            .get_memory(&source.frontmatter.id, Some("proj-1"))
            .await
            .unwrap()
            .expect("source memory exists");
        assert_eq!(
            source_doc.frontmatter.status,
            DurableMemoryStatus::Superseded
        );

        let merge_audit_path = store
            .resolver()
            .logs_dir(MemoryScope::Project, Some("proj-1"))
            .join(MERGE_AUDIT_LOG);
        let merge_audit = fs::read_to_string(merge_audit_path).await.unwrap();
        assert!(merge_audit.contains(&target.frontmatter.id));
    }

    #[tokio::test]
    async fn contradiction_marks_target_and_query_includes_all_relation_types() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let target = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze begins next week",
                "Freeze begins on Tuesday.",
                &["release".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        let superseded = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Old note",
                "Older context.",
                &["old".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        let contradiction = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Updated release note",
                "Freeze is postponed.",
                &["update".to_string()],
                Some("session-2"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        store
            .merge_memory(
                &target.frontmatter.id,
                Some("proj-1"),
                "Merged confirmation.",
                &[],
                Some("session-2"),
                "main-model",
                std::slice::from_ref(&superseded.frontmatter.id),
            )
            .await
            .unwrap();

        let contradiction_result = store
            .mark_memory_contradicted(
                &target.frontmatter.id,
                Some("proj-1"),
                std::slice::from_ref(&contradiction.frontmatter.id),
                Some("conflicting newer information"),
                Some("session-3"),
                "main-model",
            )
            .await
            .unwrap()
            .expect("target exists");
        assert!(contradiction_result.changed);
        assert_eq!(
            contradiction_result.contradicted_ids,
            vec![contradiction.frontmatter.id.clone()]
        );

        let target_doc = store
            .get_memory(&target.frontmatter.id, Some("proj-1"))
            .await
            .unwrap()
            .expect("target exists");
        assert_eq!(
            target_doc.frontmatter.status,
            DurableMemoryStatus::Contradicted
        );
        assert!(target_doc
            .frontmatter
            .relations
            .contradicted_by
            .contains(&contradiction.frontmatter.id));

        let query = store
            .query_scope(
                MemoryScope::Project,
                Some("proj-1"),
                Some("release freeze"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: true,
                },
            )
            .await
            .unwrap();
        let item = query
            .items
            .iter()
            .find(|item| item.id == target.frontmatter.id)
            .expect("target query item");
        assert!(item.related_ids.contains(&superseded.frontmatter.id));
        assert!(item.related_ids.contains(&contradiction.frontmatter.id));

        let contradiction_audit_path = store
            .resolver()
            .logs_dir(MemoryScope::Project, Some("proj-1"))
            .join(CONTRADICTION_AUDIT_LOG);
        let contradiction_audit = fs::read_to_string(contradiction_audit_path).await.unwrap();
        assert!(contradiction_audit.contains(&target.frontmatter.id));
        assert!(contradiction_audit.contains(&contradiction.frontmatter.id));
    }

    #[tokio::test]
    async fn batch_purge_updates_matching_statuses_only() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let stale_reference = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Reference,
                "Stale dashboard link",
                "Old dashboard URL.",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        let active_reference = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Reference,
                "Active dashboard link",
                "Current dashboard URL.",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        store
            .archive_memory(
                &stale_reference.frontmatter.id,
                Some("proj-1"),
                DurableMemoryStatus::Stale,
                Some("marked stale first"),
            )
            .await
            .unwrap();

        let mut type_filters = HashSet::new();
        type_filters.insert(DurableMemoryType::Reference);
        let mut status_filters = HashSet::new();
        status_filters.insert(DurableMemoryStatus::Stale);

        let result = store
            .purge_memories(
                MemoryScope::Project,
                Some("proj-1"),
                Some(&type_filters),
                Some(&status_filters),
                None,
                DurableMemoryStatus::Archived,
                Some("archive stale references"),
            )
            .await
            .unwrap();
        assert_eq!(result.matched_count, 1);
        assert_eq!(
            result.updated_ids,
            vec![stale_reference.frontmatter.id.clone()]
        );

        let stale_doc = store
            .get_memory(&stale_reference.frontmatter.id, Some("proj-1"))
            .await
            .unwrap()
            .expect("stale doc exists");
        assert_eq!(stale_doc.frontmatter.status, DurableMemoryStatus::Archived);

        let active_doc = store
            .get_memory(&active_reference.frontmatter.id, Some("proj-1"))
            .await
            .unwrap()
            .expect("active doc exists");
        assert_eq!(active_doc.frontmatter.status, DurableMemoryStatus::Active);
    }

    /// #176 (#32 nit): the resolve-then-lock methods re-read the target under the
    /// scope lock (#235), so two concurrent edits to the SAME memory id both
    /// survive. Here two `mark_memory_contradicted` calls each append a DIFFERENT
    /// source to `relations.contradicted_by`; without the re-read the second would
    /// write back its pre-lock snapshot (list without the first's entry) and lose
    /// one append. With it, both entries are present.
    #[tokio::test]
    async fn concurrent_same_memory_edits_do_not_lose_updates() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        // Helper to create a Project-scoped memory and return its id.
        async fn make(store: &MemoryStore, title: &str) -> String {
            store
                .write_memory(
                    MemoryScope::Project,
                    Some("proj-1"),
                    DurableMemoryType::Reference,
                    title,
                    "Body.",
                    &[],
                    Some("s1"),
                    "main-model",
                    false,
                    None,
                )
                .await
                .unwrap()
                .frontmatter
                .id
        }

        // Target X, and two sources A and B (mark_memory_contradicted only records
        // sources that exist in scope).
        let target = make(&store, "Target fact").await;
        let source_a = make(&store, "Contradicting fact A").await;
        let source_b = make(&store, "Contradicting fact B").await;

        // Two concurrent contradictions of the SAME target with DIFFERENT sources.
        // They serialize on the scope lock; the re-read-under-lock is what keeps
        // the second append from clobbering the first.
        let (r1, r2) = tokio::join!(
            store.mark_memory_contradicted(
                &target,
                Some("proj-1"),
                std::slice::from_ref(&source_a),
                Some("conflicts A"),
                Some("s1"),
                "main-model",
            ),
            store.mark_memory_contradicted(
                &target,
                Some("proj-1"),
                std::slice::from_ref(&source_b),
                Some("conflicts B"),
                Some("s1"),
                "main-model",
            ),
        );
        r1.unwrap().expect("first contradiction applied");
        r2.unwrap().expect("second contradiction applied");

        // BOTH appends must survive regardless of which committed first.
        let final_doc = store
            .get_memory(&target, Some("proj-1"))
            .await
            .unwrap()
            .expect("target exists");
        let contradicted = &final_doc.frontmatter.relations.contradicted_by;
        assert!(
            contradicted.contains(&source_a),
            "source A append lost (contradicted_by = {contradicted:?})"
        );
        assert!(
            contradicted.contains(&source_b),
            "source B append lost (contradicted_by = {contradicted:?})"
        );
    }

    #[tokio::test]
    async fn inspect_scope_reports_index_state_and_view_observability() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let stale_reference = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Reference,
                "Stale dashboard link",
                "Old dashboard URL.",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        store
            .archive_memory(
                &stale_reference.frontmatter.id,
                Some("proj-1"),
                DurableMemoryStatus::Stale,
                Some("marked stale first"),
            )
            .await
            .unwrap();

        let inspect = store
            .inspect_scope(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap();

        assert_eq!(inspect.total_memories, 1);
        assert!(inspect.view_files.contains(&MEMORY_VIEW_FILE.to_string()));
        assert!(inspect.view_files.contains(&RECENT_VIEW_FILE.to_string()));
        assert!(inspect.view_files.contains(&STALE_VIEW_FILE.to_string()));
        assert!(inspect.view_files.contains(&DREAM_VIEW_FILE.to_string()));
        assert!(inspect
            .index_files
            .contains(&LEXICAL_INDEX_FILE.to_string()));
        assert!(inspect.index_files.contains(&GRAPH_INDEX_FILE.to_string()));
        assert!(inspect.index_files.contains(&RECENT_INDEX_FILE.to_string()));
        assert!(inspect
            .index_files
            .contains(&STALE_CANDIDATES_INDEX_FILE.to_string()));
        assert!(inspect
            .index_files
            .contains(&TAXONOMY_INDEX_FILE.to_string()));
        assert!(inspect
            .state_files
            .contains(&"schema_version.json".to_string()));
        assert!(inspect
            .state_files
            .contains(&"last_reindex.json".to_string()));
        assert_eq!(inspect.stale_candidate_count, 1);
        assert!(inspect.last_reindex_at.is_some());
        assert!(inspect.last_dream_at.is_some());
        assert_eq!(
            inspect.recent_ids,
            vec![stale_reference.frontmatter.id.clone()]
        );
        assert_eq!(inspect.topic_paths.len(), 1);
    }

    #[tokio::test]
    async fn read_session_topics_with_content_skips_empty_topics_and_mark_session_extracted_roundtrips(
    ) {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_session_topic("session-1", "default", "primary note")
            .await
            .unwrap();
        store
            .write_session_topic("session-1", "empty", "   \n\n  ")
            .await
            .unwrap();

        let topics = store
            .read_session_topics_with_content("session-1")
            .await
            .unwrap();
        assert_eq!(
            topics,
            vec![("default".to_string(), "primary note".to_string())]
        );

        store
            .mark_session_extracted("session-1", "2026-04-05T03:00:00Z")
            .await
            .unwrap();
        let state = store.read_session_state("session-1").await.unwrap();
        assert_eq!(
            state.last_extracted_at.as_deref(),
            Some("2026-04-05T03:00:00Z")
        );
        assert!(state.topics.contains(&"default".to_string()));
        assert!(state.topics.contains(&"empty".to_string()));
    }

    #[tokio::test]
    async fn chinese_query_scope_matches_only_the_chinese_memory() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "多租户隔离设计",
                "外层编排每租户一实例,数据按文件夹隔离。",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze checklist",
                "Generic release freeze checklist for shipping work.",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        // Before #242 a Chinese query produced zero tokens and hit the
        // `return Some(1.0)` path → matched EVERY doc (matched_count == 2). Now it
        // tokenizes CJK-aware and matches only the Chinese memory.
        let result = store
            .query_scope(
                MemoryScope::Project,
                Some("proj-1"),
                Some("多租户"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(10),
                    max_chars: Some(5000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            result.matched_count, 1,
            "a distinctly-Chinese query must match only the Chinese memory, not everything"
        );
    }

    #[tokio::test]
    async fn query_scope_reports_cursor_and_truncation_across_multiple_matches() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        for idx in 0..3 {
            store
                .write_memory(
                    MemoryScope::Project,
                    Some("proj-1"),
                    DurableMemoryType::Project,
                    &format!("Release note {idx}"),
                    &format!("release freeze detail {idx}"),
                    &[],
                    Some("session-1"),
                    "main-model",
                    false,
                    None,
                )
                .await
                .unwrap();
        }

        let first = store
            .query_scope(
                MemoryScope::Project,
                Some("proj-1"),
                Some("release"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(2),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(first.returned_count, 2);
        assert_eq!(first.matched_count, 3);
        assert!(first.truncated);
        assert_eq!(first.remaining_count, 1);
        let next_cursor = first.next_cursor.clone().expect("next cursor expected");

        let second = store
            .query_scope(
                MemoryScope::Project,
                Some("proj-1"),
                Some("release"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(2),
                    max_chars: Some(3000),
                    cursor: Some(next_cursor),
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(second.returned_count, 1);
        assert_eq!(second.matched_count, 3);
        assert!(!second.truncated);
        assert_eq!(second.remaining_count, 0);
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn read_memory_views_support_project_and_global_scopes() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        store
            .write_memory(
                MemoryScope::Project,
                Some("proj-1"),
                DurableMemoryType::Project,
                "Release freeze begins next week",
                "Merge freeze begins on Tuesday for mobile release cut.",
                &["release".to_string(), "freeze".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Team handbook location",
                "Canonical team handbook lives in docs/handbook.",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let project_view = store
            .read_memory_view(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap()
            .expect("project view should exist");
        assert!(project_view.contains("Bamboo Memory Index (Project: proj-1)"));
        assert!(project_view.contains("Release freeze begins next week"));

        let global_view = store
            .read_memory_view(MemoryScope::Global, None)
            .await
            .unwrap()
            .expect("global view should exist");
        assert!(global_view.contains("Bamboo Memory Index (Global)"));
        assert!(global_view.contains("Team handbook location"));
    }

    #[tokio::test]
    async fn read_memory_view_returns_none_for_missing_or_empty_files() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        assert!(store
            .read_memory_view(MemoryScope::Project, Some("proj-missing"))
            .await
            .unwrap()
            .is_none());

        let empty_path = store
            .resolver()
            .views_dir(MemoryScope::Project, Some("proj-empty"))
            .join(MEMORY_VIEW_FILE);
        fs::create_dir_all(empty_path.parent().unwrap())
            .await
            .unwrap();
        fs::write(&empty_path, "   \n\n  ").await.unwrap();

        assert!(store
            .read_memory_view(MemoryScope::Project, Some("proj-empty"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn read_lexical_index_roundtrips_generated_index() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let doc = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-lexical"),
                DurableMemoryType::Feedback,
                "User prefers concise answers",
                "Keep responses concise and avoid unnecessary recap.",
                &["user-preference".to_string()],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();

        let lexical = store
            .read_lexical_index(MemoryScope::Project, Some("proj-lexical"))
            .await
            .unwrap()
            .expect("lexical index should exist");
        assert_eq!(lexical.items.len(), 1);
        assert_eq!(lexical.items[0].id, doc.frontmatter.id);
        assert_eq!(lexical.items[0].title, "User prefers concise answers");
        assert!(lexical.items[0].keywords.iter().any(|k| k == "concise"));
    }

    #[tokio::test]
    async fn write_memory_persists_granularity_into_document_and_lexical_index() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let doc = store
            .write_memory(
                MemoryScope::Project,
                Some("proj-temporal"),
                DurableMemoryType::Project,
                "Quarterly architecture direction",
                "This quarter we move memory recall onto a temporal granularity dimension.",
                &["architecture".to_string()],
                Some("session-1"),
                "main-model",
                false,
                Some(TemporalGranularity::Quarter),
            )
            .await
            .unwrap();
        assert_eq!(
            doc.frontmatter.granularity,
            Some(TemporalGranularity::Quarter)
        );

        // Re-read from disk: granularity must survive the markdown round-trip.
        let reread = store
            .get_memory(&doc.frontmatter.id, Some("proj-temporal"))
            .await
            .unwrap()
            .expect("memory should exist on disk");
        assert_eq!(
            reread.frontmatter.granularity,
            Some(TemporalGranularity::Quarter)
        );

        // And it must be carried into the lexical index used by recall.
        let lexical = store
            .read_lexical_index(MemoryScope::Project, Some("proj-temporal"))
            .await
            .unwrap()
            .expect("lexical index should exist");
        assert_eq!(lexical.items.len(), 1);
        assert_eq!(
            lexical.items[0].granularity,
            Some(TemporalGranularity::Quarter)
        );
    }

    #[tokio::test]
    async fn write_memory_without_granularity_defaults_to_none() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let doc = store
            .write_memory(
                MemoryScope::Global,
                None,
                DurableMemoryType::Reference,
                "Reference without temporal dimension",
                "A plain reference note that carries no granularity.",
                &[],
                Some("session-1"),
                "main-model",
                false,
                None,
            )
            .await
            .unwrap();
        assert_eq!(doc.frontmatter.granularity, None);
    }

    /// Backdate a memory's `updated_at` in place (test-only helper) by re-reading
    /// it, rewriting the timestamp, and calling the private `write_document` —
    /// mirrors the pattern already used by the merge/contradiction paths above.
    async fn backdate_memory(store: &MemoryStore, id: &str, project_key: Option<&str>, days: i64) {
        let mut doc = store
            .get_memory(id, project_key)
            .await
            .unwrap()
            .expect("memory should exist");
        doc.frontmatter.updated_at =
            (chrono::Utc::now() - chrono::Duration::days(days)).to_rfc3339();
        store.write_document(&doc).await.unwrap();
    }

    #[tokio::test]
    async fn expire_stale_granularity_marks_only_expired_day_and_week_memories() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");

        let write = |title: &'static str, granularity: Option<TemporalGranularity>| {
            let store = &store;
            async move {
                store
                    .write_memory(
                        MemoryScope::Project,
                        pk,
                        DurableMemoryType::Project,
                        title,
                        "body content for the memory",
                        &[],
                        Some("s"),
                        "m",
                        false,
                        granularity,
                    )
                    .await
                    .unwrap()
            }
        };

        let expired_day = write("Old day note", Some(TemporalGranularity::Day)).await;
        let fresh_day = write("Fresh day note", Some(TemporalGranularity::Day)).await;
        let expired_week = write("Old week note", Some(TemporalGranularity::Week)).await;
        let fresh_week = write("Fresh week note", Some(TemporalGranularity::Week)).await;
        let old_year = write("Old year note", Some(TemporalGranularity::Year)).await;
        let old_untagged = write("Old untagged note", None).await;

        backdate_memory(
            &store,
            &expired_day.frontmatter.id,
            pk,
            freshness::DAY_GRANULARITY_STALE_AFTER_DAYS + 1,
        )
        .await;
        backdate_memory(
            &store,
            &expired_week.frontmatter.id,
            pk,
            freshness::WEEK_GRANULARITY_STALE_AFTER_DAYS + 1,
        )
        .await;
        // Old enough to expire a `day` memory, but NOT a `year` or untagged one —
        // those granularities never auto-expire regardless of age.
        backdate_memory(
            &store,
            &old_year.frontmatter.id,
            pk,
            freshness::WEEK_GRANULARITY_STALE_AFTER_DAYS + 100,
        )
        .await;
        backdate_memory(
            &store,
            &old_untagged.frontmatter.id,
            pk,
            freshness::WEEK_GRANULARITY_STALE_AFTER_DAYS + 100,
        )
        .await;

        let expired_ids = store
            .expire_stale_granularity(MemoryScope::Project, pk)
            .await
            .unwrap();

        let mut expired_ids_sorted = expired_ids.clone();
        expired_ids_sorted.sort();
        let mut expected = vec![
            expired_day.frontmatter.id.clone(),
            expired_week.frontmatter.id.clone(),
        ];
        expected.sort();
        assert_eq!(expired_ids_sorted, expected);

        async fn status_of(store: &MemoryStore, id: &str, pk: Option<&str>) -> DurableMemoryStatus {
            store
                .get_memory(id, pk)
                .await
                .unwrap()
                .unwrap()
                .frontmatter
                .status
        }
        assert_eq!(
            status_of(&store, &expired_day.frontmatter.id, pk).await,
            DurableMemoryStatus::Stale
        );
        assert_eq!(
            status_of(&store, &expired_week.frontmatter.id, pk).await,
            DurableMemoryStatus::Stale
        );
        // Never touched: still fresh, still coarse/untagged.
        assert_eq!(
            status_of(&store, &fresh_day.frontmatter.id, pk).await,
            DurableMemoryStatus::Active
        );
        assert_eq!(
            status_of(&store, &fresh_week.frontmatter.id, pk).await,
            DurableMemoryStatus::Active
        );
        assert_eq!(
            status_of(&store, &old_year.frontmatter.id, pk).await,
            DurableMemoryStatus::Active,
            "year granularity must never auto-expire, however old"
        );
        assert_eq!(
            status_of(&store, &old_untagged.frontmatter.id, pk).await,
            DurableMemoryStatus::Active,
            "untagged memories must never be silently reclassified"
        );

        // Non-destructive: nothing was deleted, all 6 documents remain on disk.
        let docs = store
            .list_memory_documents(MemoryScope::Project, pk)
            .await
            .unwrap();
        assert_eq!(docs.len(), 6);

        // A second run is a no-op: Stale docs are not re-processed (Active only).
        let second_run = store
            .expire_stale_granularity(MemoryScope::Project, pk)
            .await
            .unwrap();
        assert!(second_run.is_empty());
    }

    #[tokio::test]
    async fn query_scope_filters_by_granularity_and_absent_filter_is_back_compat() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());
        let pk = Some("proj-1");

        store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "This week's priorities",
                "Ship the granularity filter.",
                &[],
                Some("s"),
                "m",
                false,
                Some(TemporalGranularity::Week),
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "Long-term direction",
                "Move to a modular workspace layout.",
                &[],
                Some("s"),
                "m",
                false,
                Some(TemporalGranularity::Year),
            )
            .await
            .unwrap();
        store
            .write_memory(
                MemoryScope::Project,
                pk,
                DurableMemoryType::Project,
                "Untagged note",
                "No temporal dimension set.",
                &[],
                Some("s"),
                "m",
                false,
                None,
            )
            .await
            .unwrap();

        let mut week_only = HashSet::new();
        week_only.insert(TemporalGranularity::Week);
        let filtered = store
            .query_scope(
                MemoryScope::Project,
                pk,
                None,
                None,
                None,
                Some(&week_only),
                &MemoryQueryOptions {
                    limit: Some(10),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(filtered.matched_count, 1);
        assert_eq!(filtered.items[0].title, "This week's priorities");

        // Absent filter (`None`) = old behavior: every memory matches.
        let unfiltered = store
            .query_scope(
                MemoryScope::Project,
                pk,
                None,
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(10),
                    max_chars: Some(3000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(unfiltered.matched_count, 3);
    }

    #[tokio::test]
    async fn project_dream_view_roundtrips_and_updates_project_state_marker() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let path = store
            .write_project_dream_view(
                "proj-dream",
                "# Bamboo Dream Notebook\n\nProject-only dream",
            )
            .await
            .expect("write project dream");
        assert!(path.ends_with("scopes/projects/proj-dream/views/DREAM_NOTEBOOK.md"));

        let dream = store
            .read_project_dream_view("proj-dream")
            .await
            .expect("read project dream")
            .expect("project dream should exist");
        assert!(dream.contains("Project-only dream"));

        let state_marker = store
            .resolver()
            .state_dir(MemoryScope::Project, Some("proj-dream"))
            .join("last_dream.json");
        let raw = fs::read_to_string(state_marker)
            .await
            .expect("read state marker");
        assert!(raw.contains("updated_at"));
    }

    #[tokio::test]
    async fn missing_project_dream_view_returns_none_and_global_behavior_remains_unchanged() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        assert!(store
            .read_project_dream_view("proj-missing")
            .await
            .expect("read project dream")
            .is_none());

        store
            .write_dream_view("# Bamboo Dream Notebook\n\nGlobal dream remains intact")
            .await
            .expect("write global dream");
        let global = store
            .read_dream_view()
            .await
            .expect("read global dream")
            .expect("global dream should exist");
        assert!(global.contains("Global dream remains intact"));
    }

    #[tokio::test]
    async fn append_session_topic_serializes_concurrent_appends() {
        // Without the per-topic lock, concurrent read-modify-write appends drop
        // each other's updates; with it, every entry survives (#235).
        let dir = tempdir().unwrap();
        const WRITERS: usize = 16;

        let mut handles = Vec::new();
        for i in 0..WRITERS {
            let path = dir.path().to_path_buf();
            handles.push(tokio::spawn(async move {
                let store = MemoryStore::new(&path);
                store
                    .append_session_topic("sess-concurrent", "topic", &format!("entry-{i}"))
                    .await
                    .expect("append succeeds");
            }));
        }
        for handle in handles {
            handle.await.expect("writer task joins");
        }

        let store = MemoryStore::new(dir.path());
        let content = store
            .read_session_topic("sess-concurrent", "topic")
            .await
            .expect("read topic")
            .unwrap_or_default();
        // Exact section match (entries are joined by "\n\n") — a substring check
        // would let "entry-1" pass on the presence of "entry-10".
        let sections: Vec<&str> = content.split("\n\n").map(str::trim).collect();
        for i in 0..WRITERS {
            let expected = format!("entry-{i}");
            assert!(
                sections.iter().any(|section| *section == expected),
                "append {expected} was lost to a race (sections: {sections:?})"
            );
        }
    }
}
