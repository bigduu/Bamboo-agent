use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome};
use bamboo_storage::SessionStoreV2;

mod args;
mod context_view;
mod handlers;
mod helpers;

use args::SessionInspectorArgs;

/// The history capability granted to one tool surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionHistoryAccess {
    /// Search only the authoritative caller Session.
    SelfOnly,
    /// Preserve the complete Root viewer in addition to self-search.
    Full,
}

/// Server-only tool for inspecting V2 sessions stored under the Bamboo home dir.
///
/// Design goals:
/// - Return metadata first (index-backed) so the model can narrow scope.
/// - Allow bounded reads (pagination; from end; truncation).
/// - Support lightweight search across session titles and (optionally) tail messages.
/// - Keep inspection local by default; use child-session delegation only if the user explicitly asks.
pub struct SessionInspectorTool {
    pub(super) session_store: Arc<SessionStoreV2>,
    pub(super) storage: Arc<dyn Storage>,
    access: SessionHistoryAccess,
}

impl SessionInspectorTool {
    pub fn new(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            storage,
            access: SessionHistoryAccess::Full,
        }
    }

    /// Construct the least-privilege surface used by Base and Child sessions.
    pub fn self_only(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            storage,
            access: SessionHistoryAccess::SelfOnly,
        }
    }

    pub(super) async fn load_session(
        &self,
        session_id: &str,
    ) -> Result<bamboo_agent_core::Session, ToolError> {
        match self.storage.load_session(session_id).await {
            Ok(Some(s)) => Ok(s),
            Ok(None) => Err(ToolError::Execution(format!(
                "session not found: {session_id}"
            ))),
            Err(e) => Err(ToolError::Execution(format!(
                "failed to load session {session_id}: {e}"
            ))),
        }
    }
}

#[async_trait]
impl Tool for SessionInspectorTool {
    fn name(&self) -> &str {
        "session_history"
    }

    fn description(&self) -> &str {
        match self.access {
            SessionHistoryAccess::SelfOnly => {
                "Read-only search over the current Bamboo Session's own stored messages, including compressed history. Scope is derived from trusted runtime context; no Session ID or compressed-state recovery is accepted from the caller."
            }
            SessionHistoryAccess::Full => {
                "Read-only viewer over local session history. Search the current Session directly (including compressed messages), or list sessions, inspect metadata, read bounded message slices/compressed history, and search prior conversations. A Root caller can use export_context for itself or a same-tree, same-Project target: it materializes bounded immutable status/brief files for Read offset/limit, without changing session state. Exported status is a last persisted observation, not verified live progress. This viewer has no runtime control. Distinct from memory, which manages durable cross-session knowledge."
            }
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        if self.access == SessionHistoryAccess::SelfOnly {
            return json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["search_current"],
                        "description": "Search the current Session's own stored messages."
                    },
                    "query": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 512,
                        "description": "Literal or lexical message-content query."
                    },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50, "description": "Maximum matches to return (default 20)." }
                },
                "required": ["action", "query"],
                "additionalProperties": false
            });
        }

        // Keep schema permissive; Rust parsing enforces action-specific requirements.
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["search_current", "list", "get_meta", "read_messages", "read_compressed_cache", "search", "export_context"],
                    "description": "Which inspection action to perform."
                },
                "query": { "type": "string", "description": "Search string (search_current/list/search)." },
                "kind": { "type": "string", "enum": ["root", "child"], "description": "Filter by session kind (list)." },
                "pinned": { "type": "boolean", "description": "Filter pinned sessions (list)." },
                "parent_session_id": { "type": "string", "description": "Filter child sessions by parent (list)." },
                "root_session_id": { "type": "string", "description": "Filter by root session (list)." },
                "created_by_schedule_id": { "type": "string", "description": "Filter sessions created by a schedule (list)." },
                "limit": { "type": "number", "description": "Max items/messages to return (search_current/list/read_messages)." },
                "offset": { "type": "number", "description": "Offset (list/read_messages)." },
                "session_id": { "type": "string", "description": "Target session id. export_context requires a persisted Root caller and a target in its own tree with the same optional Project identity; output paths are runtime-owned." },
                "from_end": { "type": "boolean", "description": "Read from end (read_messages)." },
                "truncate_chars": { "type": "number", "description": "Max chars per message (read_messages)." },
                "include_system": { "type": "boolean" },
                "include_tool": { "type": "boolean" },
                "include_tool_calls": { "type": "boolean" },
                "include_image_urls": { "type": "boolean" },
                "include_summary": { "type": "boolean", "description": "Include cached conversation summary when available (read_compressed_cache)." },
                "mode": { "type": "string", "enum": ["title", "tail_messages"] },
                "max_sessions": { "type": "number" },
                "tail_messages": { "type": "number" },
                "case_sensitive": { "type": "boolean" },
                "max_matches": { "type": "number" }
            },
            "required": ["action"]
        })
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let caller_session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution(
                "session_history requires a session_id in tool context".to_string(),
            )
        })?;

        let action = args.get("action").and_then(serde_json::Value::as_str);
        if self.access == SessionHistoryAccess::SelfOnly && action != Some("search_current") {
            return Err(ToolError::InvalidArguments(
                "this session_history surface only permits action=search_current for the caller's own Session"
                    .to_string(),
            ));
        }
        if action == Some("search_current")
            && args.as_object().is_some_and(|fields| {
                fields
                    .keys()
                    .any(|key| !matches!(key.as_str(), "action" | "query" | "limit"))
            })
        {
            return Err(ToolError::InvalidArguments(
                "search_current only accepts action, query, and limit; Session scope and compressed-message inclusion are runtime-derived"
                    .to_string(),
            ));
        }

        if action == Some("export_context")
            && args.as_object().is_some_and(|fields| {
                fields
                    .keys()
                    .any(|key| !matches!(key.as_str(), "action" | "session_id"))
            })
        {
            return Err(ToolError::InvalidArguments(
                "export_context only accepts action and session_id; caller identity and output paths are runtime-derived".to_string(),
            ));
        }
        let parsed: SessionInspectorArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid session_history args: {e}"))
        })?;

        match parsed {
            SessionInspectorArgs::SearchCurrent { query, limit } => {
                handlers::handle_search_current(
                    self,
                    caller_session_id,
                    ctx.tool_call_id.as_ref(),
                    query,
                    limit,
                )
                .await
            }
            SessionInspectorArgs::ExportContext { session_id } => {
                context_view::export_context(self, caller_session_id, &session_id).await
            }
            SessionInspectorArgs::List {
                query,
                kind,
                pinned,
                parent_session_id,
                root_session_id,
                created_by_schedule_id,
                limit,
                offset,
            } => {
                handlers::handle_list(
                    self,
                    query,
                    kind,
                    pinned,
                    parent_session_id,
                    root_session_id,
                    created_by_schedule_id,
                    limit,
                    offset,
                )
                .await
            }

            SessionInspectorArgs::GetMeta { session_id } => {
                handlers::handle_get_meta(self, session_id).await
            }

            SessionInspectorArgs::ReadMessages {
                session_id,
                from_end,
                offset,
                limit,
                truncate_chars,
                include_system,
                include_tool,
                include_tool_calls,
                include_image_urls,
            } => {
                handlers::handle_read_messages(
                    self,
                    session_id,
                    from_end,
                    offset,
                    limit,
                    truncate_chars,
                    include_system,
                    include_tool,
                    include_tool_calls,
                    include_image_urls,
                )
                .await
            }

            SessionInspectorArgs::ReadCompressedCache {
                session_id,
                offset,
                limit,
                truncate_chars,
                include_summary,
            } => {
                handlers::handle_read_compressed_cache(
                    self,
                    session_id,
                    offset,
                    limit,
                    truncate_chars,
                    include_summary,
                )
                .await
            }

            SessionInspectorArgs::Search {
                query,
                mode,
                max_sessions,
                tail_messages,
                case_sensitive,
                max_matches,
            } => {
                handlers::handle_search(
                    self,
                    query,
                    mode,
                    max_sessions,
                    tail_messages,
                    case_sensitive,
                    max_matches,
                )
                .await
            }
        }
        .map(ToolOutcome::Completed)
    }
}
