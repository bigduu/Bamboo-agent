use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

use bamboo_memory::memory_store::{
    DurableMemoryStatus, DurableMemoryType, MemoryQueryOptions, MemoryScope, MemoryStore,
    MAX_MAX_CHARS, MAX_QUERY_LIMIT,
};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_agent_core::Session;
use bamboo_tools::tools::session_memory::{
    execute_session_memory_action, SessionMemoryAction, MEMORY_SESSION_ACTION_NAMES,
};

type FilterTypeSet = (
    Option<HashSet<DurableMemoryType>>,
    Option<HashSet<DurableMemoryStatus>>,
);

#[derive(Clone)]
pub struct MemoryTool {
    sessions: Arc<RwLock<std::collections::HashMap<String, Session>>>,
    storage: Arc<dyn Storage>,
    memory_store: MemoryStore,
}

impl MemoryTool {
    pub fn new(
        sessions: Arc<RwLock<std::collections::HashMap<String, Session>>>,
        storage: Arc<dyn Storage>,
        data_dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            sessions,
            storage,
            memory_store: MemoryStore::new(data_dir),
        }
    }

    async fn session_for_context(&self, session_id: Option<&str>) -> Option<Session> {
        let session_id = session_id?;
        let in_memory = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };
        match in_memory {
            Some(session) => Some(session),
            None => self.storage.load_session(session_id).await.ok().flatten(),
        }
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

    fn parse_scope(scope: Option<&str>) -> Result<MemoryScope, ToolError> {
        match scope
            .unwrap_or("session")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "session" => Ok(MemoryScope::Session),
            "project" => Ok(MemoryScope::Project),
            "global" => Ok(MemoryScope::Global),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid scope '{other}'; expected one of: session, project, global"
            ))),
        }
    }

    fn parse_type(value: &str) -> Result<DurableMemoryType, ToolError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "user" => Ok(DurableMemoryType::User),
            "feedback" => Ok(DurableMemoryType::Feedback),
            "project" => Ok(DurableMemoryType::Project),
            "reference" => Ok(DurableMemoryType::Reference),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid type '{other}'; expected one of: user, feedback, project, reference"
            ))),
        }
    }

    fn parse_status(value: &str) -> Result<DurableMemoryStatus, ToolError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "active" => Ok(DurableMemoryStatus::Active),
            "stale" => Ok(DurableMemoryStatus::Stale),
            "superseded" => Ok(DurableMemoryStatus::Superseded),
            "contradicted" => Ok(DurableMemoryStatus::Contradicted),
            "archived" => Ok(DurableMemoryStatus::Archived),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid status '{other}'; expected one of: active, stale, superseded, contradicted, archived"
            ))),
        }
    }

    fn parse_query_filters(
        filters: Option<&QueryFilters>,
    ) -> Result<FilterTypeSet, ToolError> {
        let filter_types = filters
            .map(|value| {
                value
                    .r#type
                    .iter()
                    .map(|item| Self::parse_type(item))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        let filter_statuses = filters
            .map(|value| {
                value
                    .status
                    .iter()
                    .map(|item| Self::parse_status(item))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        Ok((filter_types, filter_statuses))
    }

    fn parse_merge_mode(value: Option<&str>) -> Result<Option<String>, ToolError> {
        let Some(mode) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let normalized = mode.to_ascii_lowercase();
        match normalized.as_str() {
            "semantic_merge" | "merge" | "contradict" => Ok(Some(normalized)),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid merge mode '{other}'; expected one of: merge, semantic_merge, contradict"
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum MemoryArgs {
    SessionRead {
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    SessionAppend {
        #[serde(default)]
        topic: Option<String>,
        content: String,
    },
    SessionReplace {
        #[serde(default)]
        topic: Option<String>,
        content: String,
    },
    SessionClear {
        #[serde(default)]
        topic: Option<String>,
    },
    SessionListTopics,
    Query {
        scope: String,
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        filters: Option<QueryFilters>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    Get {
        id: String,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    Write {
        scope: String,
        #[serde(rename = "type")]
        r#type: String,
        title: String,
        content: String,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<WriteOptions>,
    },
    Merge {
        id: String,
        content: String,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        source_memory_ids: Vec<String>,
        #[serde(default)]
        mode: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    },
    Purge {
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        scope: Option<String>,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        filters: Option<QueryFilters>,
        #[serde(default)]
        mode: Option<String>,
    },
    Inspect {
        scope: String,
        #[serde(default)]
        project_key: Option<String>,
    },
    Rebuild {
        scope: String,
        #[serde(default)]
        project_key: Option<String>,
    },
}

#[derive(Debug, Deserialize, Default)]
struct MemoryActionOptions {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    max_chars: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    include_related: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct QueryFilters {
    #[serde(default)]
    r#type: Vec<String>,
    #[serde(default)]
    status: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct WriteOptions {
    #[serde(default)]
    allow_merge_if_similar: Option<bool>,
}

#[async_trait]
impl Tool for MemoryTool {
    fn name(&self) -> &str {
        "memory"
    }

    fn description(&self) -> &str {
        "Unified memory management tool for Bamboo. Use session_* actions for session continuity notes, and query/get/write/purge/inspect/rebuild for durable project/global memory backed by canonical topic files and derived indexes."
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
                        "write",
                        "merge",
                        "purge",
                        "inspect",
                        "rebuild"
                    ]
                },
                "scope": {"type": "string", "enum": ["session", "project", "global"]},
                "project_key": {"type": "string"},
                "topic": {"type": "string"},
                "id": {"type": "string"},
                "query": {"type": "string"},
                "type": {"type": "string", "enum": ["user", "feedback", "project", "reference"]},
                "title": {"type": "string"},
                "content": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "filters": {"type": "object"},
                "options": {"type": "object"},
                "reason": {"type": "string"}
            },
            "required": ["action"]
        })
    }

    fn call_mutability(&self, args: &serde_json::Value) -> bamboo_tools::ToolMutability {
        let action = args
            .get("action")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match action.as_str() {
            "session_read" | "session_list_topics" | "query" | "get" | "inspect" => {
                bamboo_tools::ToolMutability::ReadOnly
            }
            _ => bamboo_tools::ToolMutability::Mutating,
        }
    }

    fn call_concurrency_safe(&self, args: &serde_json::Value) -> bool {
        matches!(
            self.call_mutability(args),
            bamboo_tools::ToolMutability::ReadOnly
        )
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution("memory requires a session_id in tool context".to_string())
        })?;

        let parsed: MemoryArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid memory args: {error}"))
        })?;

        match parsed {
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
                let (filter_types, filter_statuses) = Self::parse_query_filters(filters.as_ref())?;
                let result = self
                    .memory_store
                    .query_scope(
                        scope,
                        project_key.as_deref(),
                        query.as_deref(),
                        filter_types.as_ref(),
                        filter_statuses.as_ref(),
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
                })
            }
            MemoryArgs::Write {
                scope,
                r#type,
                title,
                content,
                tags,
                project_key,
                options,
            } => {
                let scope = Self::parse_scope(Some(&scope))?;
                if scope == MemoryScope::Session {
                    return Err(ToolError::InvalidArguments(
                        "write supports durable scopes only; use session_replace/session_append for session scope"
                            .to_string(),
                    ));
                }
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
                            .unwrap_or(true),
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
                    })
                }
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
                    })
                } else {
                    let scope = Self::parse_scope(scope.as_deref())?;
                    if scope == MemoryScope::Session {
                        return Err(ToolError::InvalidArguments(
                            "purge supports durable scopes only in v1".to_string(),
                        ));
                    }
                    let (filter_types, filter_statuses) =
                        Self::parse_query_filters(filters.as_ref())?;
                    let result = self
                        .memory_store
                        .purge_memories(
                            scope,
                            project_key.as_deref(),
                            filter_types.as_ref(),
                            filter_statuses.as_ref(),
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
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use tokio::sync::RwLock;

    #[derive(Default)]
    struct TestStorage {
        sessions: RwLock<HashMap<String, Session>>,
    }

    #[async_trait]
    impl Storage for TestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.read().await.get(session_id).cloned())
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.write().await.remove(session_id).is_some())
        }
    }

    fn test_context<'a>(session_id: &'a str) -> ToolExecutionContext<'a> {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id: "tool-call-1",
            event_tx: None,
            available_tool_schemas: None,
        }
    }

    fn build_memory_tool(data_dir: &std::path::Path) -> MemoryTool {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        MemoryTool::new(sessions, storage, data_dir)
    }

    #[tokio::test]
    async fn memory_session_actions_share_read_shape_and_limits() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tool = build_memory_tool(dir.path());

        tool.execute_with_context(
            json!({"action":"session_replace","topic":"default","content":"x".repeat(32)}),
            test_context("session-1"),
        )
        .await
        .expect("session replace should succeed");

        let read = tool
            .execute_with_context(
                json!({"action":"session_read","topic":"default","options":{"max_chars":8}}),
                test_context("session-1"),
            )
            .await
            .expect("session read should succeed");
        let value: serde_json::Value = serde_json::from_str(&read.result).expect("valid json");
        assert_eq!(value["action"], "session_read");
        assert_eq!(value["length_chars"], 32);
        assert_eq!(value["body_truncated"], true);
        assert_eq!(value["content"].as_str().unwrap().chars().count(), 8);
    }

    #[tokio::test]
    async fn memory_session_append_enforces_shared_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tool = build_memory_tool(dir.path());

        tool.execute_with_context(
            json!({
                "action":"session_replace",
                "topic":"limit",
                "content":"x".repeat(bamboo_tools::tools::session_memory::MAX_SESSION_NOTE_CHARS - 1)
            }),
            test_context("session-2"),
        )
        .await
        .expect("session replace near limit should succeed");

        let err = tool
            .execute_with_context(
                json!({"action":"session_append","topic":"limit","content":"y"}),
                test_context("session-2"),
            )
            .await
            .expect_err("session append should fail");
        let message = err.to_string();
        assert!(message.contains("session note would exceed the limit"));
        assert!(message.contains("action=session_read"));
        assert!(message.contains("action=session_replace"));
    }

    #[tokio::test]
    async fn memory_session_list_topics_includes_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tool = build_memory_tool(dir.path());

        tool.execute_with_context(
            json!({"action":"session_append","topic":"alpha","content":"A"}),
            test_context("session-3"),
        )
        .await
        .expect("session append should succeed");
        tool.execute_with_context(
            json!({"action":"session_append","topic":"beta","content":"B"}),
            test_context("session-3"),
        )
        .await
        .expect("session append should succeed");

        let list = tool
            .execute_with_context(
                json!({"action":"session_list_topics"}),
                test_context("session-3"),
            )
            .await
            .expect("session list topics should succeed");
        let value: serde_json::Value = serde_json::from_str(&list.result).expect("valid json");
        assert_eq!(value["action"], "session_list_topics");
        assert_eq!(value["count"], 2);
    }
}
