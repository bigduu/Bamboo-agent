use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use tokio::fs;

use bamboo_agent_core::workspace_state;

use super::{
    build_dream_view, build_memory_markdown_view, build_recent_markdown_view,
    build_stale_markdown_view, derive_summary, detect_entities, extract_keywords,
    make_query_cursor, match_memory_query, normalize_tags, now_rfc3339, parse_markdown_document,
    parse_query_cursor, parse_rfc3339, project_key_from_path, render_markdown_document,
    short_stable_hash, sort_memories_desc, validate_memory_title, validate_session_id,
    validate_session_topic, AuditLogEntry, GraphIndex, GraphIndexItem, LexicalIndex,
    LexicalIndexItem, MemoryContradictionResult, MemoryInspectResult, MemoryMergeResult,
    MemoryPathResolver, MemoryPurgeResult, MemoryQueryItem, MemoryQueryOptions, MemoryQueryResult,
    RecentIndex, RecentIndexItem, StaleCandidateItem, StaleCandidatesIndex, TaxonomyIndex,
    CONTRADICTION_AUDIT_LOG, DEFAULT_MAX_CHARS, DEFAULT_QUERY_LIMIT, DEFAULT_SESSION_TOPIC,
    DREAM_VIEW_FILE, GRAPH_INDEX_FILE, LEXICAL_INDEX_FILE, MAX_MAX_CHARS, MAX_QUERY_LIMIT,
    MEMORY_SCHEMA_VERSION, MEMORY_VIEW_FILE, MERGE_AUDIT_LOG, PURGE_AUDIT_LOG, RECENT_INDEX_FILE,
    RECENT_VIEW_FILE, STALE_CANDIDATES_INDEX_FILE, STALE_VIEW_FILE, TAXONOMY_INDEX_FILE,
    WRITE_AUDIT_LOG,
};
use super::{
    CreatedBy, DurableMemoryDocument, DurableMemoryFrontmatter, DurableMemoryRelations,
    DurableMemoryRetrieval, DurableMemorySource, DurableMemoryStatus, DurableMemoryType,
    MemoryScope, SessionState,
};

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
            resolver: MemoryPathResolver::from_data_dir(bamboo_infrastructure::paths::bamboo_dir()),
        }
    }

    pub fn resolver(&self) -> &MemoryPathResolver {
        &self.resolver
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
        fs::write(&path, content).await?;
        self.persist_session_state(session_id).await?;
        Ok(path)
    }

    pub async fn append_session_topic(
        &self,
        session_id: &str,
        topic: &str,
        content: &str,
    ) -> io::Result<PathBuf> {
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

    pub async fn query_scope(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        query: Option<&str>,
        filter_types: Option<&HashSet<DurableMemoryType>>,
        filter_statuses: Option<&HashSet<DurableMemoryStatus>>,
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
                let relevance = match_memory_query(&doc, query, filter_types, filter_statuses)?;
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
                .map(|doc| bamboo_infrastructure::paths::path_to_display_string(&doc.path))
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

        self.ensure_scope_dirs(scope, project_key).await?;
        let tags = normalize_tags(tags.iter().map(String::as_str));

        if allow_merge_if_similar {
            if let Some(mut existing) = self
                .find_similar_memory(scope, project_key, r#type, title, &tags)
                .await?
            {
                if !existing.body.contains(content) {
                    existing.body = format!("{}\n\n---\n\n{}", existing.body.trim_end(), content);
                }
                existing.frontmatter.updated_at = now_rfc3339();
                existing.frontmatter.updated_by = CreatedBy {
                    kind: "memory_write".to_string(),
                    id: None,
                    actor: Some(actor.to_string()),
                };
                let mut merged_tags = existing.frontmatter.tags.clone();
                merged_tags.extend(tags.clone());
                existing.frontmatter.tags = normalize_tags(merged_tags.iter().map(String::as_str));
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
            relations: DurableMemoryRelations::default(),
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

    pub async fn purge_memories(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        filter_types: Option<&HashSet<DurableMemoryType>>,
        filter_statuses: Option<&HashSet<DurableMemoryStatus>>,
        mode: DurableMemoryStatus,
        reason: Option<&str>,
    ) -> io::Result<MemoryPurgeResult> {
        let project_key = self.require_project_key(scope, project_key)?;
        let mut docs = self.list_memory_documents(scope, project_key).await?;
        let mut updated_ids = Vec::new();
        for doc in &mut docs {
            if match_memory_query(doc, None, filter_types, filter_statuses).is_none() {
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
            doc.body = format!("{}\n\n---\n\n{}", doc.body.trim_end(), content);
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

    async fn find_similar_memory(
        &self,
        scope: MemoryScope,
        project_key: Option<&str>,
        r#type: DurableMemoryType,
        title: &str,
        tags: &[String],
    ) -> io::Result<Option<DurableMemoryDocument>> {
        let normalized_title = super::sanitize_component(title);
        let title_keywords: std::collections::HashSet<String> =
            extract_keywords(title, "", tags).into_iter().collect();
        let title_entities: std::collections::HashSet<String> =
            detect_entities(title, "").into_iter().collect();
        let docs = self.list_memory_documents(scope, project_key).await?;

        let mut best_exact: Option<DurableMemoryDocument> = None;
        let mut best_heuristic: Option<(usize, DurableMemoryDocument)> = None;

        for doc in docs.into_iter().filter(|doc| {
            doc.frontmatter.status == DurableMemoryStatus::Active
                && doc.frontmatter.r#type == r#type
        }) {
            let doc_normalized_title = super::sanitize_component(&doc.frontmatter.title);
            if doc_normalized_title == normalized_title {
                best_exact = Some(doc);
                break;
            }

            let doc_keywords: std::collections::HashSet<String> =
                doc.frontmatter.retrieval.keywords.iter().cloned().collect();
            let doc_entities: std::collections::HashSet<String> =
                doc.frontmatter.retrieval.entities.iter().cloned().collect();

            let keyword_overlap = title_keywords.intersection(&doc_keywords).count();
            let entity_overlap = title_entities.intersection(&doc_entities).count();
            let title_prefix_match = doc_normalized_title.starts_with(&normalized_title)
                || normalized_title.starts_with(&doc_normalized_title);

            let score = keyword_overlap + (entity_overlap * 2) + usize::from(title_prefix_match);
            if score >= 4 {
                match &best_heuristic {
                    Some((best_score, _)) if *best_score >= score => {}
                    _ => best_heuristic = Some((score, doc)),
                }
            }
        }

        Ok(best_exact.or_else(|| best_heuristic.map(|(_, doc)| doc)))
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
        fs::write(&doc.path, rendered).await
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
                })
                .collect(),
        };
        self.write_json_file(
            self.resolver
                .indexes_dir(scope, project_key)
                .join(LEXICAL_INDEX_FILE),
            &lexical,
        )
        .await?;

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
        self.write_json_file(
            self.resolver
                .indexes_dir(scope, project_key)
                .join(RECENT_INDEX_FILE),
            &recent,
        )
        .await?;

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
        self.write_json_file(
            self.resolver
                .indexes_dir(scope, project_key)
                .join(GRAPH_INDEX_FILE),
            &graph,
        )
        .await?;

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
        self.write_json_file(
            self.resolver
                .indexes_dir(scope, project_key)
                .join(STALE_CANDIDATES_INDEX_FILE),
            &stale,
        )
        .await?;

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
        self.write_json_file(
            self.resolver
                .indexes_dir(scope, project_key)
                .join(TAXONOMY_INDEX_FILE),
            &taxonomy,
        )
        .await?;

        let views_dir = self.resolver.views_dir(scope, project_key);
        fs::create_dir_all(&views_dir).await?;
        fs::write(
            views_dir.join(MEMORY_VIEW_FILE),
            build_memory_markdown_view(scope, project_key, &docs),
        )
        .await?;
        fs::write(
            views_dir.join(RECENT_VIEW_FILE),
            build_recent_markdown_view(&docs),
        )
        .await?;
        fs::write(
            views_dir.join(STALE_VIEW_FILE),
            build_stale_markdown_view(&docs),
        )
        .await?;
        let dream_path = views_dir.join(DREAM_VIEW_FILE);
        if !dream_path.exists() {
            fs::write(&dream_path, build_dream_view(None)).await?;
        }

        self.write_json_file(
            self.resolver
                .state_dir(scope, project_key)
                .join("schema_version.json"),
            &serde_json::json!({ "version": super::MEMORY_SCHEMA_VERSION }),
        )
        .await?;
        self.write_json_file(
            self.resolver
                .state_dir(scope, project_key)
                .join("last_reindex.json"),
            &serde_json::json!({ "updated_at": now, "count": docs.len() }),
        )
        .await?;

        Ok(())
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
        fs::create_dir_all(self.resolver.locks_dir(scope, project_key)).await?;
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
                    fs::write(&target, content).await?;
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
                                fs::write(&target, content).await?;
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
        fs::write(&view_path, build_dream_view(Some(&content))).await?;
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
        let line = serde_json::to_string(&entry).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize audit log: {error}"),
            )
        })?;
        let mut existing = if path.exists() {
            fs::read_to_string(&path).await?
        } else {
            String::new()
        };
        existing.push_str(&line);
        existing.push('\n');
        fs::write(path, existing).await
    }

    async fn write_json_file<T: serde::Serialize>(
        &self,
        path: PathBuf,
        value: &T,
    ) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let data = serde_json::to_vec_pretty(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to serialize json: {error}"),
            )
        })?;
        fs::write(path, data).await
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
        self.ensure_scope_dirs(scope, project_key).await?;
        let path = self
            .resolver
            .views_dir(scope, project_key)
            .join(DREAM_VIEW_FILE);
        fs::write(&path, build_dream_view(Some(content))).await?;
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

    #[tokio::test]
    async fn write_memory_merges_into_heuristically_similar_active_memory() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::new(dir.path());

        let original = store
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
            )
            .await
            .unwrap();

        let merged = store
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
            )
            .await
            .unwrap();

        assert_eq!(merged.frontmatter.id, original.frontmatter.id);
        assert!(merged
            .body
            .contains("Merge freeze begins on Tuesday for mobile release cut."));
        assert!(merged
            .body
            .contains("Stakeholders confirmed the mobile release freeze starts Tuesday."));
        assert!(merged.frontmatter.tags.contains(&"mobile".to_string()));
        assert!(merged.frontmatter.tags.contains(&"release".to_string()));

        let docs = store
            .list_memory_documents(MemoryScope::Project, Some("proj-1"))
            .await
            .unwrap();
        assert_eq!(
            docs.len(),
            1,
            "heuristically similar writes should merge into one durable memory"
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
                &[source.frontmatter.id.clone()],
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
                &[superseded.frontmatter.id.clone()],
            )
            .await
            .unwrap();

        let contradiction_result = store
            .mark_memory_contradicted(
                &target.frontmatter.id,
                Some("proj-1"),
                &[contradiction.frontmatter.id.clone()],
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
}
