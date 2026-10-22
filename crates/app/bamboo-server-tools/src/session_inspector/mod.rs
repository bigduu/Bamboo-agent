use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome};
use bamboo_storage::SessionStoreV2;

mod args;
mod context_view;
pub mod discovery;
mod handlers;
mod helpers;
mod self_history;

use args::SessionInspectorArgs;
use discovery::{
    AuthorizedSessionInspectRequest, AuthorizedSessionListRequest, SessionDiscoveryPort,
    SupervisorSessionDiscovery,
};

const SESSION_HISTORY_TOOL_NAME: &str = "session_history";
const SESSION_HISTORY_CURRENT_TOOL_NAME: &str = "session_history_current";

/// The history capability granted to one tool surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionHistoryAccess {
    /// Search and page only the authoritative caller Session.
    SelfOnly,
    /// Preserve the complete Root viewer in addition to self-history reads.
    Full,
    /// Only metadata discovery and bounded progress through an installed grant adapter.
    Authorized,
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
    discovery: Arc<dyn SessionDiscoveryPort>,
}

impl SessionInspectorTool {
    pub fn new(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            discovery: Arc::new(SupervisorSessionDiscovery::new(storage.clone())),
            storage,
            access: SessionHistoryAccess::Full,
            tool_name: SESSION_HISTORY_TOOL_NAME,
        }
    }

    /// A role-neutral, restricted surface. The installed Host adapter must
    /// authorize every page/read; unsupported callers never reach the global index.
    pub fn authorized(
        session_store: Arc<SessionStoreV2>,
        storage: Arc<dyn Storage>,
        discovery: Arc<dyn SessionDiscoveryPort>,
    ) -> Self {
        Self {
            session_store,
            storage,
            access: SessionHistoryAccess::Authorized,
            tool_name: SESSION_HISTORY_TOOL_NAME,
            discovery,
        }
    }

    /// Construct the least-privilege surface used by Base and Child sessions.
    pub fn self_only(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            discovery: Arc::new(SupervisorSessionDiscovery::new(storage.clone())),
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
            discovery: Arc::new(SupervisorSessionDiscovery::new(storage.clone())),
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
                "Read-only viewer over local session history. A Supervisor can list only its explicitly authorized targets with a small limit and cursor, inspect bounded persisted progress, or read_messages/get_meta for an attached Root. Directory queries match target IDs or Projects; stale cursors require a new first page. Inspect reports session-scoped persisted evidence, never verified live or input-specific completion. Ordinary legacy viewers retain current-Session search/paging, local history listing and same-tree export_context. This viewer has no runtime control. Send follow-up through session_message(action=send) and its separately installed Host authority; a directory entry does not grant sending rights."
            }
            SessionHistoryAccess::Authorized => {
                "Read-only discovery of explicitly Host-authorized Sessions. list returns metadata-only pages (limit <=20, query matches target ID/Project); reuse the returned cursor with the same query and limit. inspect returns bounded persisted progress and a visible assistant excerpt, never verified live or input-specific completion. Caller identity and grants come from the installed Host adapter. Missing/stale authority is a blocker; this surface never falls back to all local Sessions, attaches history or sends work. Use session_message(action=send) with its own Host authority for delivery; this listing is not a sending grant."
            }
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        if self.access == SessionHistoryAccess::Authorized {
            // OpenAI transports strip top-level combinators. Keep every
            // usable field at the top; execution enforces action fields too.
            return json!({
                "type":"object",
                "properties": {
                    "action":{"type":"string","enum":["list","inspect"]},
                    "query":{"type":"string","maxLength":128,"description":"Target ID or Project filter for list."},
                    "limit":{"type":"integer","minimum":1,"maximum":20},
                    "cursor":{"type":"string","maxLength":4096},
                    "session_id":{"type":"string","minLength":1,"maxLength":256},
                    "max_chars":{"type":"integer","minimum":1,"maximum":8000}
                },
                "required":["action"],
                "additionalProperties":false,
                "oneOf": [
                    {
                        "type":"object",
                        "properties": {
                            "action":{"const":"list"},
                            "query":{"type":"string","maxLength":128,"description":"Target ID or Project filter."},
                            "limit":{"type":"integer","minimum":1,"maximum":20},
                            "cursor":{"type":"string","maxLength":4096}
                        },
                        "required":["action"],
                        "additionalProperties":false
                    },
                    {
                        "type":"object",
                        "properties": {
                            "action":{"const":"inspect"},
                            "session_id":{"type":"string","minLength":1,"maxLength":256},
                            "max_chars":{"type":"integer","minimum":1,"maximum":8000}
                        },
                        "required":["action","session_id"],
                        "additionalProperties":false
                    }
                ]
            });
        }
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
                    "enum": ["search_current", "read_current", "read_around", "list", "inspect", "get_meta", "read_messages", "read_compressed_cache", "search", "export_context"],
                    "description": "Which inspection action to perform."
                },
                "query": { "type": "string", "description": "Search string (search_current/list/search)." },
                "kind": { "type": "string", "enum": ["root", "child"], "description": "Filter by session kind (list)." },
                "pinned": { "type": "boolean", "description": "Filter pinned sessions (list)." },
                "parent_session_id": { "type": "string", "description": "Filter child sessions by parent (list)." },
                "root_session_id": { "type": "string", "description": "Filter by root session (list)." },
                "created_by_schedule_id": { "type": "string", "description": "Filter sessions created by a schedule (list)." },
                "limit": { "type": "number", "description": "Max items/messages to return (search_current/list/read_messages)." },
                "cursor": { "type": "string", "description": "Opaque continuation from read_current or an authorized list. Authorized list cursors require the original query and limit." },
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
        let scoped_discovery = self.access == SessionHistoryAccess::Authorized
            || (self.access == SessionHistoryAccess::Full
                && (ctx.executing_supervisor.is_some()
                    || caller_session_id == bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID));
        if self.access == SessionHistoryAccess::Authorized
            && !matches!(action, Some("list" | "inspect"))
        {
            return Err(ToolError::InvalidArguments(
                "authorized Session discovery permits only list and inspect".into(),
            ));
        }
        if scoped_discovery
            && matches!(action, Some("list" | "inspect"))
            && args.as_object().is_some_and(|fields| {
                fields.keys().any(|key| match action {
                    Some("list") => {
                        !matches!(key.as_str(), "action" | "query" | "limit" | "cursor")
                    }
                    Some("inspect") => {
                        !matches!(key.as_str(), "action" | "session_id" | "max_chars")
                    }
                    _ => false,
                })
            })
        {
            return Err(ToolError::InvalidArguments(
                "authorized list accepts only action/query/limit/cursor; inspect accepts only action/session_id/max_chars".into(),
            ));
        }
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

        if scoped_discovery
            && matches!(
                &parsed,
                SessionInspectorArgs::List { .. } | SessionInspectorArgs::Inspect { .. }
            )
        {
            let value = match parsed {
                SessionInspectorArgs::List {
                    query,
                    limit,
                    cursor,
                    ..
                } => {
                    let request = AuthorizedSessionListRequest {
                        query,
                        limit,
                        cursor,
                    };
                    request.validate()?;
                    serde_json::to_value(self.discovery.list(&ctx, request).await?)
                }
                SessionInspectorArgs::Inspect {
                    session_id,
                    max_chars,
                } => {
                    let request = AuthorizedSessionInspectRequest {
                        session_id,
                        max_chars,
                    };
                    request.validate()?;
                    serde_json::to_value(self.discovery.inspect(&ctx, request).await?)
                }
                _ => unreachable!("discovery actions checked"),
            }
            .map_err(|error| {
                ToolError::Execution(format!("serialize Session discovery: {error}"))
            })?;
            return Ok(ToolOutcome::Completed(
                bamboo_agent_core::tools::ToolResult::text(true, value.to_string()),
            ));
        }

        if self.access == SessionHistoryAccess::Full
            && (ctx.executing_supervisor.is_some()
                || caller_session_id == bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        {
            let original = ctx
                .executing_supervisor_for(caller_session_id)
                .ok_or_else(|| {
                    ToolError::Execution(
                        "Supervisor history requires the original executing identity".into(),
                    )
                })?
                .supervisor_reference();
            let target = match &parsed {
                SessionInspectorArgs::ReadMessages { session_id, .. }
                | SessionInspectorArgs::GetMeta { session_id } => session_id,
                _ => return Err(ToolError::InvalidArguments(
                    "Supervisor history supports authorized list/inspect and bounded read_messages/get_meta for its own Session or an explicitly attached Root; use session_history_current for current-turn paging".into(),
                )),
            };
            let session = self
                .storage
                .load_supervisor_managed_session(&original, target)
                .await
                .map_err(|error| {
                    ToolError::Execution(format!("Supervisor history denied: {error}"))
                })?
                .ok_or_else(|| ToolError::Execution("managed history target is missing".into()))?;
            let result = match parsed {
                SessionInspectorArgs::ReadMessages { session_id, from_end, offset, limit, truncate_chars, include_system, include_tool, include_tool_calls, include_image_urls } => {
                    handlers::handle_read_messages(self, Some(session), session_id, from_end, offset, limit, truncate_chars, include_system, include_tool, include_tool_calls, include_image_urls).await?
                }
                SessionInspectorArgs::GetMeta { .. } => bamboo_agent_core::tools::ToolResult::text(true, json!({"session":{
                    "id":session.id,"kind":session.kind,"root_session_id":session.root_session_id,
                    "parent_session_id":session.parent_session_id,"project_id":session.project_id_meta(),
                    "model":session.model,"message_count":session.messages.len(),"created_at":session.created_at,
                    "metadata_version":session.metadata_version
                }}).to_string()),
                _ => unreachable!("scoped action checked before the physical read"),
            };
            return Ok(ToolOutcome::Completed(result));
        }

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
                cursor,
            } => {
                if cursor.is_some() {
                    return Err(ToolError::InvalidArguments(
                        "list cursors require an installed authorized Session discovery adapter".into(),
                    ));
                }
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

            SessionInspectorArgs::Inspect { .. } => Err(ToolError::Execution(
                "session_discovery_authority_required: inspect requires an installed authorized Session discovery adapter".into(),
            )),

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
                    None,
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
