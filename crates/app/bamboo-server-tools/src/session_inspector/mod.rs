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
mod self_history;

use args::SessionInspectorArgs;

const SESSION_HISTORY_TOOL_NAME: &str = "session_history";
const SESSION_HISTORY_CURRENT_TOOL_NAME: &str = "session_history_current";

/// The history capability granted to one tool surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionHistoryAccess {
    /// Search and page only the authoritative caller Session.
    SelfOnly,
    /// Preserve the complete Root viewer in addition to self-history reads.
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
    tool_name: &'static str,
}

impl SessionInspectorTool {
    pub fn new(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            storage,
            access: SessionHistoryAccess::Full,
            tool_name: SESSION_HISTORY_TOOL_NAME,
        }
    }

    /// Construct the least-privilege surface used by Base and Child sessions.
    pub fn self_only(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            storage,
            access: SessionHistoryAccess::SelfOnly,
            tool_name: SESSION_HISTORY_TOOL_NAME,
        }
    }

    /// Construct the exact, always-resident current-Session identity.
    ///
    /// This is intentionally not an alias for `session_history`: exact tool
    /// registration is the capability-loading and authorization boundary.
    pub fn current(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            storage,
            access: SessionHistoryAccess::SelfOnly,
            tool_name: SESSION_HISTORY_CURRENT_TOOL_NAME,
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
        self.tool_name
    }

    fn description(&self) -> &str {
        match self.access {
            SessionHistoryAccess::SelfOnly => {
                "Read-only search and bounded exact-turn retrieval over the current Bamboo Session's own stored messages, including compressed history. Scope is derived from trusted runtime context; no Session ID or compressed-state recovery is accepted from the caller. Use search_current, then read_around or read_current; raw Session history is transcript authority while memory is selective and may be stale."
            }
            SessionHistoryAccess::Full => {
                "Read-only viewer over local session history. Search/page/read around the current Session directly (including compressed messages), or list sessions, inspect metadata, read bounded message slices/compressed history, and search prior conversations. A Root caller can use export_context for itself or a same-tree, same-Project target: it materializes bounded immutable status/brief files for Read offset/limit, without changing session state. Exported status is a last persisted observation, not verified live progress. This viewer has no runtime control. Distinct from memory, which manages durable cross-session knowledge."
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
                        "enum": ["search_current", "read_current", "read_around"],
                        "description": "Search, page, or recover complete logical turns around a hit in the current Session only."
                    },
                    "query": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 512,
                        "description": "Literal or lexical message-content query."
                    },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50, "description": "Maximum search matches or logical turns to return. read_current has a hard maximum of 20 turns." },
                    "cursor": { "type": "string", "maxLength": 4096, "description": "Opaque continuation cursor returned by read_current." },
                    "direction": { "type": "string", "enum": ["backward", "forward"], "description": "Pagination direction for read_current (default backward)." },
                    "max_chars": { "type": "integer", "minimum": 1, "maximum": 20000, "description": "Hard character budget for exact content and tool arguments (default 12000)." },
                    "archived_only": { "type": "boolean", "description": "For read_current, select only logical turns containing archived messages while retaining active companion messages needed for turn/tool-chain atomicity." },
                    "message_id": { "type": "string", "minLength": 1, "maxLength": 512, "description": "Message ID returned by search_current to anchor read_around." },
                    "before_turns": { "type": "integer", "minimum": 0, "maximum": 5, "description": "Complete turns before the anchor (default 1)." },
                    "after_turns": { "type": "integer", "minimum": 0, "maximum": 5, "description": "Complete turns after the anchor (default 1)." }
                },
                "required": ["action"],
                "additionalProperties": false
            });
        }

        // Keep schema permissive; Rust parsing enforces action-specific requirements.
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["search_current", "read_current", "read_around", "list", "get_meta", "read_messages", "read_compressed_cache", "search", "export_context"],
                    "description": "Which inspection action to perform."
                },
                "query": { "type": "string", "description": "Search string (search_current/list/search)." },
                "kind": { "type": "string", "enum": ["root", "child"], "description": "Filter by session kind (list)." },
                "pinned": { "type": "boolean", "description": "Filter pinned sessions (list)." },
                "parent_session_id": { "type": "string", "description": "Filter child sessions by parent (list)." },
                "root_session_id": { "type": "string", "description": "Filter by root session (list)." },
                "created_by_schedule_id": { "type": "string", "description": "Filter sessions created by a schedule (list)." },
                "limit": { "type": "number", "description": "Max items/messages to return (search_current/list/read_messages)." },
                "cursor": { "type": "string", "description": "Opaque continuation cursor returned by read_current." },
                "direction": { "type": "string", "enum": ["backward", "forward"] },
                "max_chars": { "type": "number", "description": "Hard total content budget for read_current/read_around." },
                "archived_only": { "type": "boolean", "description": "Select logical turns containing archived messages for read_current." },
                "message_id": { "type": "string", "description": "Current-Session message ID returned by search_current (read_around)." },
                "before_turns": { "type": "number", "description": "Complete turns before the read_around anchor." },
                "after_turns": { "type": "number", "description": "Complete turns after the read_around anchor." },
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
            ToolError::Execution(format!(
                "{} requires a session_id in tool context",
                self.name()
            ))
        })?;

        let action = args.get("action").and_then(serde_json::Value::as_str);
        if self.access == SessionHistoryAccess::SelfOnly
            && !matches!(
                action,
                Some("search_current" | "read_current" | "read_around")
            )
        {
            return Err(ToolError::InvalidArguments(
                format!(
                    "this {} surface only permits search_current, read_current, and read_around for the caller's own Session",
                    self.name()
                ),
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
        if action == Some("read_current")
            && args.as_object().is_some_and(|fields| {
                fields.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "action" | "cursor" | "direction" | "limit" | "max_chars" | "archived_only"
                    )
                })
            })
        {
            return Err(ToolError::InvalidArguments(
                "read_current only accepts action, cursor, direction, limit, max_chars, and archived_only; Session scope and history boundary are runtime-derived"
                    .to_string(),
            ));
        }
        if action == Some("read_around")
            && args.as_object().is_some_and(|fields| {
                fields.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "action" | "message_id" | "before_turns" | "after_turns" | "max_chars"
                    )
                })
            })
        {
            return Err(ToolError::InvalidArguments(
                "read_around only accepts action, message_id, before_turns, after_turns, and max_chars; Session scope and history boundary are runtime-derived"
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
            ToolError::InvalidArguments(format!("Invalid {} args: {e}", self.name()))
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
            SessionInspectorArgs::ReadCurrent {
                cursor,
                direction,
                limit,
                max_chars,
                archived_only,
            } => {
                self_history::handle_read_current(
                    self,
                    caller_session_id,
                    ctx.tool_call_id.as_ref(),
                    cursor,
                    direction,
                    limit,
                    max_chars,
                    archived_only,
                )
                .await
            }
            SessionInspectorArgs::ReadAround {
                message_id,
                before_turns,
                after_turns,
                max_chars,
            } => {
                self_history::handle_read_around(
                    self,
                    caller_session_id,
                    ctx.tool_call_id.as_ref(),
                    message_id,
                    before_turns,
                    after_turns,
                    max_chars,
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
