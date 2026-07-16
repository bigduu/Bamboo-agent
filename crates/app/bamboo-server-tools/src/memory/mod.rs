use async_trait::async_trait;
use serde_json::json;

use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_agent_core::Session;
use bamboo_memory::memory_store::{
    DurableMemoryStatus, MemoryQueryOptions, MemoryScope, MemoryStore, MAX_MAX_CHARS,
    MAX_QUERY_LIMIT,
};
use bamboo_tools::tools::session_memory::{
    execute_session_memory_action, SessionMemoryAction, MEMORY_SESSION_ACTION_NAMES,
};

mod args;
mod parsing;

#[cfg(test)]
mod tests;

use args::MemoryArgs;

#[derive(Clone)]
pub struct MemoryTool {
    session_repo: bamboo_engine::SessionRepository,
    memory_store: MemoryStore,
}

impl MemoryTool {
    pub fn new(
        session_repo: bamboo_engine::SessionRepository,
        data_dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            session_repo,
            memory_store: MemoryStore::new(data_dir),
        }
    }

    async fn session_for_context(&self, session_id: Option<&str>) -> Option<Session> {
        self.session_repo.load(session_id?).await
    }

    async fn resolve_project_key(
        &self,
        explicit: Option<&str>,
        session_id: Option<&str>,
    ) -> Option<String> {
        if let Some(explicit) = explicit
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
        {
            return Some(explicit);
        }

        if let Some(project_key) = self.memory_store.project_key_for_session(session_id) {
            return Some(project_key);
        }

        self.session_for_context(session_id)
            .await
            .and_then(|session| session.metadata.get("workspace_path").cloned())
            .map(std::path::PathBuf::from)
            .map(|path| bamboo_memory::memory_store::project_key_from_path(&path))
    }
}

#[async_trait]
impl Tool for MemoryTool {
    fn name(&self) -> &str {
        "memory"
    }

    fn description(&self) -> &str {
        "Unified memory management tool for Bamboo. Use session_* actions for session continuity notes, and query/get/write/merge/split/consolidate/purge/inspect/rebuild for durable project/global memory backed by canonical topic files and derived indexes."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "session_read",
                        "session_append",
                        "session_replace",
                        "session_clear",
                        "session_list_topics",
                        "query",
                        "get",
                        "find_duplicates",
                        "write",
                        "merge",
                        "split",
                        "consolidate",
                        "purge",
                        "inspect",
                        "rebuild",
                        "scan_blobs",
                        "scan_duplicates"
                    ]
                },
                "scope": {"type": "string", "enum": ["session", "project", "global"]},
                "granularity": {
                    "type": "string",
                    "enum": ["day", "week", "month", "quarter", "year"],
                    "description": "Optional temporal granularity for `write`, orthogonal to scope: day (today's working context), week (sprint), month, quarter (direction), year (long-term goals). Omit if the memory has no time horizon. Coarser granularities are prefix-cache friendly and recalled ahead of finer ones at equal relevance."
                },
                "project_key": {"type": "string"},
                "topic": {"type": "string"},
                "id": {"type": "string"},
                "query": {"type": "string"},
                "type": {"type": "string", "enum": ["user", "feedback", "project", "reference"]},
                "title": {"type": "string"},
                "content": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "pieces": {"type": "array", "items": {"type": "object"}},
                "ids": {"type": "array", "items": {"type": "string"}},
                "min_score": {"type": "number"},
                "filters": {
                    "type": "object",
                    "description": "Optional narrowing for `query`/`purge`. Each sub-filter is independent and defaults to unfiltered when omitted or empty.",
                    "properties": {
                        "type": {
                            "type": "array",
                            "items": {"type": "string", "enum": ["user", "feedback", "project", "reference"]},
                            "description": "Restrict results to these memory types. Omit for no type filtering."
                        },
                        "status": {
                            "type": "array",
                            "items": {"type": "string", "enum": ["active", "stale", "superseded", "contradicted", "archived"]},
                            "description": "Restrict results to these statuses. Omit for no status filtering."
                        },
                        "granularity": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["day", "week", "month", "quarter", "year"]
                            },
                            "description": "Restrict results to these temporal granularities: day (today's working context), week (sprint), month, quarter (direction), year (long-term goals). This is a hard filter (unlike the passive recall-into-prompt ordering): a memory whose granularity doesn't appear in this list is excluded entirely, and a memory with no granularity never matches a non-empty filter here. Omit for no granularity filtering."
                        }
                    }
                },
                "options": {"type": "object"},
                "reason": {"type": "string"}
            },
            "required": ["action"]
        })
    }

    fn classify(&self, args: &serde_json::Value) -> ToolClass {
        let action = args
            .get("action")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match action.as_str() {
            "session_read"
            | "session_list_topics"
            | "query"
            | "get"
            | "find_duplicates"
            | "scan_blobs"
            | "scan_duplicates"
            | "inspect" => ToolClass::READONLY_PARALLEL,
            _ => ToolClass::MUTATING_SERIAL,
        }
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("memory requires a session_id in tool context".to_string())
        })?;

        let parsed: MemoryArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid memory args: {error}"))
        })?;

        let result = match parsed {
            MemoryArgs::SessionRead { topic, options } => {
                let max_chars = options.and_then(|value| value.max_chars);
                execute_session_memory_action(
                    &self.memory_store,
                    session_id,
                    SessionMemoryAction::Read,
                    topic.as_deref(),
                    None,
                    max_chars,
                    MEMORY_SESSION_ACTION_NAMES,
                )
                .await
            }
            MemoryArgs::SessionAppend { topic, content } => {
                execute_session_memory_action(
                    &self.memory_store,
                    session_id,
                    SessionMemoryAction::Append,
                    topic.as_deref(),
                    Some(content.as_str()),
                    None,
                    MEMORY_SESSION_ACTION_NAMES,
                )
                .await
            }
            MemoryArgs::SessionReplace { topic, content } => {
                execute_session_memory_action(
                    &self.memory_store,
                    session_id,
                    SessionMemoryAction::Replace,
                    topic.as_deref(),
                    Some(content.as_str()),
                    None,
                    MEMORY_SESSION_ACTION_NAMES,
                )
                .await
            }
            MemoryArgs::SessionClear { topic } => {
                execute_session_memory_action(
                    &self.memory_store,
                    session_id,
                    SessionMemoryAction::Clear,
                    topic.as_deref(),
                    None,
                    None,
                    MEMORY_SESSION_ACTION_NAMES,
                )
                .await
            }
            MemoryArgs::SessionListTopics => {
                execute_session_memory_action(
                    &self.memory_store,
                    session_id,
                    SessionMemoryAction::ListTopics,
                    None,
                    None,
                    None,
                    MEMORY_SESSION_ACTION_NAMES,
                )
                .await
            }
            MemoryArgs::Query {
                scope,
                query,
                filters,
                project_key,
                options,
            } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "query supports durable scopes only; use session_read/session_list_topics for session scope"
                            .to_string(),
                    ));
                }
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let options = MemoryQueryOptions {
                    limit: options
                        .as_ref()
                        .and_then(|value| value.limit)
                        .map(|value| value.min(MAX_QUERY_LIMIT)),
                    max_chars: options
                        .as_ref()
                        .and_then(|value| value.max_chars)
                        .map(|value| value.min(MAX_MAX_CHARS)),
                    cursor: options.as_ref().and_then(|value| value.cursor.clone()),
                    include_related: options
                        .as_ref()
                        .and_then(|value| value.include_related)
                        .unwrap_or(false),
                };
                let (filter_types, filter_statuses, filter_granularity) =
                    Self::parse_query_filters(filters.as_ref())?;
                let result = self
                    .memory_store
                    .query_scope(
                        scope,
                        project_key.as_deref(),
                        query.as_deref(),
                        filter_types.as_ref(),
                        filter_statuses.as_ref(),
                        filter_granularity.as_ref(),
                        &options,
                    )
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to query memory: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "query",
                        "success": true,
                        "data": result,
                        "summary": bamboo_memory::memory_store::summary_json(result.returned_count, result.matched_count),
                        "warnings": [],
                    }).to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Get {
                id,
                project_key,
                options,
            } => {
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let max_chars = options
                    .and_then(|value| value.max_chars)
                    .unwrap_or(MAX_MAX_CHARS)
                    .min(MAX_MAX_CHARS);
                let Some(mut doc) = self
                    .memory_store
                    .get_memory(id.trim(), project_key.as_deref())
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to get memory: {error}"))
                    })?
                else {
                    return Err(ToolError::Execution(format!(
                        "memory not found: {}",
                        id.trim()
                    )));
                };
                let (body, truncated) =
                    bamboo_memory::memory_store::truncate_chars(&doc.body, max_chars);
                doc.body = body;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "get",
                        "id": doc.frontmatter.id,
                        "memory": {
                            "frontmatter": doc.frontmatter,
                            "body": doc.body,
                            "path": doc.path,
                            "body_truncated": truncated,
                        }
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Write {
                scope,
                r#type,
                title,
                content,
                tags,
                project_key,
                granularity,
                options,
            } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "write supports durable scopes only; use session_replace/session_append for session scope"
                            .to_string(),
                    ));
                }
                let granularity = Self::parse_granularity(granularity.as_deref())?;
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let doc = self
                    .memory_store
                    .write_memory(
                        scope,
                        project_key.as_deref(),
                        Self::parse_type(&r#type)?,
                        &title,
                        &content,
                        &tags,
                        Some(session_id),
                        "main-model",
                        options
                            .and_then(|value| value.allow_merge_if_similar)
                            .unwrap_or(false),
                        granularity,
                    )
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to write memory: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "write",
                        "memory": {
                            "id": doc.frontmatter.id,
                            "title": doc.frontmatter.title,
                            "type": doc.frontmatter.r#type,
                            "scope": doc.frontmatter.scope,
                            "status": doc.frontmatter.status,
                            "project_key": doc.frontmatter.project_key,
                            "path": doc.path,
                        }
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Merge {
                id,
                content,
                tags,
                project_key,
                source_memory_ids,
                mode,
                reason,
            } => {
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let mode = Self::parse_merge_mode(mode.as_deref())?;
                if matches!(mode.as_deref(), Some("contradict")) {
                    let Some(result) = self
                        .memory_store
                        .mark_memory_contradicted(
                            id.trim(),
                            project_key.as_deref(),
                            &source_memory_ids,
                            reason.as_deref().or(Some(content.trim())),
                            Some(session_id),
                            "main-model",
                        )
                        .await
                        .map_err(|error| {
                            ToolError::Execution(format!("Failed to contradict memory: {error}"))
                        })?
                    else {
                        return Err(ToolError::Execution(format!(
                            "memory not found: {}",
                            id.trim()
                        )));
                    };
                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "action": "merge",
                            "mode": "contradict",
                            "data": result,
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                        images: Vec::new(),
                    })
                } else {
                    let Some(result) = self
                        .memory_store
                        .merge_memory(
                            id.trim(),
                            project_key.as_deref(),
                            &content,
                            &tags,
                            Some(session_id),
                            "main-model",
                            &source_memory_ids,
                        )
                        .await
                        .map_err(|error| {
                            ToolError::Execution(format!("Failed to merge memory: {error}"))
                        })?
                    else {
                        return Err(ToolError::Execution(format!(
                            "memory not found: {}",
                            id.trim()
                        )));
                    };
                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "action": "merge",
                            "mode": mode.unwrap_or_else(|| "merge".to_string()),
                            "data": result,
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                        images: Vec::new(),
                    })
                }
            }
            MemoryArgs::FindDuplicates {
                scope,
                title,
                content,
                r#type,
                tags,
                project_key,
                options,
            } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "find_duplicates supports durable scopes only".to_string(),
                    ));
                }
                let r#type = match r#type.as_deref() {
                    Some(value) => Some(Self::parse_type(value)?),
                    None => None,
                };
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let limit = options
                    .and_then(|value| value.limit)
                    .unwrap_or(5)
                    .clamp(1, MAX_QUERY_LIMIT);
                let candidates = self
                    .memory_store
                    .find_duplicate_candidates(
                        scope,
                        project_key.as_deref(),
                        r#type,
                        &title,
                        content.as_deref().unwrap_or(""),
                        &tags,
                        limit,
                    )
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to find duplicates: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "find_duplicates",
                        "candidates": candidates,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Split {
                id,
                project_key,
                pieces,
            } => {
                if pieces.is_empty() {
                    return Err(ToolError::InvalidArguments(
                        "split requires at least one piece".to_string(),
                    ));
                }
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let mut split_pieces = Vec::with_capacity(pieces.len());
                for piece in pieces {
                    let r#type = match piece.r#type.as_deref() {
                        Some(value) => Some(Self::parse_type(value)?),
                        None => None,
                    };
                    split_pieces.push(bamboo_memory::memory_store::MemorySplitPiece {
                        title: piece.title,
                        r#type,
                        content: piece.content,
                        tags: piece.tags,
                    });
                }
                let Some(result) = self
                    .memory_store
                    .split_memory(
                        id.trim(),
                        project_key.as_deref(),
                        &split_pieces,
                        Some(session_id),
                        "main-model",
                    )
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to split memory: {error}"))
                    })?
                else {
                    return Err(ToolError::Execution(format!(
                        "memory not found: {}",
                        id.trim()
                    )));
                };
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "split",
                        "data": result,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::ScanBlobs {
                scope,
                project_key,
                min_sections,
                options,
            } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "scan_blobs supports durable scopes only".to_string(),
                    ));
                }
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let min_sections = min_sections.unwrap_or(3);
                let limit = options
                    .and_then(|value| value.limit)
                    .unwrap_or(20)
                    .clamp(1, 200);
                let report = self
                    .memory_store
                    .scan_blob_candidates(scope, project_key.as_deref(), min_sections, limit)
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to scan blobs: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "scan_blobs",
                        "report": report,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::ScanDuplicates {
                scope,
                project_key,
                min_score,
                options,
            } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "scan_duplicates supports durable scopes only".to_string(),
                    ));
                }
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let min_score = min_score.unwrap_or(0.6);
                let limit = options
                    .and_then(|value| value.limit)
                    .unwrap_or(20)
                    .clamp(1, 200);
                let report = self
                    .memory_store
                    .scan_duplicate_clusters(scope, project_key.as_deref(), min_score, 5, limit)
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to scan duplicates: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "scan_duplicates",
                        "report": report,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Consolidate {
                ids,
                title,
                content,
                r#type,
                tags,
                project_key,
            } => {
                if ids.len() < 2 {
                    return Err(ToolError::InvalidArguments(
                        "consolidate requires at least two source memory ids".to_string(),
                    ));
                }
                let r#type = match r#type.as_deref() {
                    Some(value) => Some(Self::parse_type(value)?),
                    None => None,
                };
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let merged = bamboo_memory::memory_store::MemorySplitPiece {
                    title,
                    r#type,
                    content,
                    tags,
                };
                let ids: Vec<String> = ids.iter().map(|id| id.trim().to_string()).collect();
                let Some(result) = self
                    .memory_store
                    .consolidate_memories(
                        &ids,
                        project_key.as_deref(),
                        &merged,
                        Some(session_id),
                        "main-model",
                    )
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to consolidate memories: {error}"))
                    })?
                else {
                    return Err(ToolError::Execution(
                        "one or more source memories not found".to_string(),
                    ));
                };
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "consolidate",
                        "data": result,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Purge {
                id,
                scope,
                reason,
                project_key,
                filters,
                mode,
            } => {
                let mode = match mode
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    Some(value) => Self::parse_status(value)?,
                    None => DurableMemoryStatus::Archived,
                };
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;

                if let Some(id) = id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    let Some(doc) = self
                        .memory_store
                        .archive_memory(id, project_key.as_deref(), mode, reason.as_deref())
                        .await
                        .map_err(|error| {
                            ToolError::Execution(format!("Failed to purge memory: {error}"))
                        })?
                    else {
                        return Err(ToolError::Execution(format!("memory not found: {}", id)));
                    };
                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "action": "purge",
                            "id": doc.frontmatter.id,
                            "status": doc.frontmatter.status,
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                        images: Vec::new(),
                    })
                } else {
                    let scope = Self::parse_scope(scope.as_deref())?;
                    if scope == MemoryScope::Session {
                        return Err(ToolError::InvalidArguments(
                            "purge supports durable scopes only in v1".to_string(),
                        ));
                    }
                    let (filter_types, filter_statuses, filter_granularity) =
                        Self::parse_query_filters(filters.as_ref())?;
                    let result = self
                        .memory_store
                        .purge_memories(
                            scope,
                            project_key.as_deref(),
                            filter_types.as_ref(),
                            filter_statuses.as_ref(),
                            filter_granularity.as_ref(),
                            mode,
                            reason.as_deref(),
                        )
                        .await
                        .map_err(|error| {
                            ToolError::Execution(format!("Failed to purge memory: {error}"))
                        })?;
                    Ok(ToolResult {
                        success: true,
                        result: json!({
                            "action": "purge",
                            "data": result,
                        })
                        .to_string(),
                        display_preference: Some("json".to_string()),
                        images: Vec::new(),
                    })
                }
            }
            MemoryArgs::Inspect { scope, project_key } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "inspect supports durable scopes only in v1".to_string(),
                    ));
                }
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                let result = self
                    .memory_store
                    .inspect_scope(scope, project_key.as_deref())
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to inspect memory: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "inspect",
                        "data": result,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
            MemoryArgs::Rebuild { scope, project_key } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "rebuild supports durable scopes only in v1".to_string(),
                    ));
                }
                let project_key = self
                    .resolve_project_key(project_key.as_deref(), Some(session_id))
                    .await;
                self.memory_store
                    .rebuild_scope(scope, project_key.as_deref())
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to rebuild memory artifacts: {error}"))
                    })?;
                let inspect = self
                    .memory_store
                    .inspect_scope(scope, project_key.as_deref())
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Failed to inspect rebuilt memory: {error}"))
                    })?;
                Ok(ToolResult {
                    success: true,
                    result: json!({
                        "action": "rebuild",
                        "scope": scope,
                        "project_key": project_key,
                        "data": inspect,
                    })
                    .to_string(),
                    display_preference: Some("json".to_string()),
                    images: Vec::new(),
                })
            }
        }?;
        Ok(ToolOutcome::Completed(result))
    }
}
