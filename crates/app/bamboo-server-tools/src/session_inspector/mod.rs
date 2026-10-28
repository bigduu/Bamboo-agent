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
pub mod host_discovery;
mod scoped_context;
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
    /// Self reads plus Host-routed list/inspect. No ordinary raw peer history.
    HostRouted,
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
    /// Production discovery routing: installed Host scopes take precedence for
    /// every role. Legacy Supervisor history requires never-installed absence.
    pub fn host_routed(session_store: Arc<SessionStoreV2>, storage: Arc<dyn Storage>) -> Self {
        Self {
            session_store,
            discovery: Arc::new(host_discovery::SessionDiscoveryRouter::new(storage.clone())),
            storage,
            access: SessionHistoryAccess::HostRouted,
            tool_name: SESSION_HISTORY_TOOL_NAME,
        }
    }

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

    /// Trusted Host subscription adapter. It reuses the exact scoped reader and
    /// safe publisher, skipping publication when selected meaningful facts match.
    pub async fn observe_context(
        &self,
        ctx: &ToolCtx,
        target_id: &str,
        filter: &std::collections::BTreeSet<bamboo_domain::ObserverChangeKind>,
        previous: Option<&str>,
    ) -> Result<Option<bamboo_agent_core::tools::ToolResult>, ToolError> {
        scoped_context::observe(self, ctx, target_id, filter, previous).await
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
                "Read-only history and discovery. A canonical Supervisor can list all Sessions in the application's current-user domain with bounded pages/cursors, inspect persisted progress, and read bounded Root/Child history without Project authorization or attachment. Directory entries and inspect include current_user_target: preserve it as target_ref for a later followup; the final sender separately checks caller and target lifetime/generation/ancestry. Query matches IDs or Projects; stale cursors require a fresh page. Persisted progress is not verified live or input-specific completion. Ordinary viewers retain current-Session paging/search and their existing same-tree or finite Host rights. This viewer has no execution or file authority."
            }
            SessionHistoryAccess::HostRouted => {
                "Read-only Session discovery. A canonical Supervisor uses the current-user domain resolver regardless of legacy Project lists, attachments or old finite Host scope; its original identity and source generation are checked on every operation. Other callers retain explicitly installed Host scopes: list/inspect provide bounded metadata/progress, and export_context/read_context require their independent ReadContextView right and fixed selectors. Revoked/unsupported ordinary scopes never fall back to broader access. IDs, Projects and cursors grant nothing. Use current-Session paging/search for current history. This tool attaches no history, sends no input and grants no raw file or execution authority."
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
        let mut schema = json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["search_current", "read_current", "read_around", "list", "inspect", "get_meta", "read_messages", "read_compressed_cache", "search", "export_context", "read_context"],
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
                "session_id": { "type": "string", "description": "Target session ID. Legacy export_context is same-tree/same-Project; scoped export_context/read_context require the original caller and current explicit ReadContextView authority (or the original managed Supervisor route). Paths are runtime-derived." },
                "revision": { "type":"string", "minLength":64, "maxLength":64, "description":"Exact immutable view revision returned by scoped export_context." },
                "file": { "type":"string", "enum":["status","brief"], "description":"Fixed context file for read_context; no path is accepted." },
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
        });
        if self.access == SessionHistoryAccess::HostRouted {
            schema["properties"]["action"]["enum"] = json!([
                "search_current",
                "read_current",
                "read_around",
                "list",
                "inspect",
                "get_meta",
                "read_messages",
                "read_compressed_cache",
                "export_context",
                "read_context"
            ]);
            schema["properties"]["session_id"]["description"] = json!(
                "inspect requires InspectBoundedProgress; export_context/read_context separately require ReadContextView. An original managed Supervisor may use its independent never-installed-Host compatibility route. read_context accepts only exact revision, status/brief, offset<=120 and limit1..40. Other actions are self-only, except existing managed get_meta/read_messages for a real Supervisor with never-installed Host scope."
            );
            schema["properties"]["query"]["description"] = json!(
                "Target ID/Project filter for list, or text in current-Session search_current."
            );
            schema["properties"]["max_chars"]["description"] = json!("Bounded progress excerpt for inspect (1..8000), or existing current-turn budget for read_current/read_around.");
            if let Some(properties) = schema["properties"].as_object_mut() {
                for key in [
                    "kind",
                    "pinned",
                    "parent_session_id",
                    "root_session_id",
                    "created_by_schedule_id",
                    "mode",
                    "max_sessions",
                    "tail_messages",
                    "case_sensitive",
                    "max_matches",
                ] {
                    properties.remove(key);
                }
            }
        }
        schema
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
        // Preserve the existing ordinary Root self-export. Intrinsic self
        // history is not a fallback from a denied peer scope.
        let legacy_self_export = self.access == SessionHistoryAccess::HostRouted
            && action == Some("export_context")
            && args.get("session_id").and_then(serde_json::Value::as_str)
                == Some(caller_session_id)
            && ctx.executing_supervisor.is_none()
            && caller_session_id != bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID;
        let context_route = (self.access == SessionHistoryAccess::HostRouted
            && !legacy_self_export)
            || (self.access == SessionHistoryAccess::Full
                && (ctx.executing_supervisor.is_some()
                    || caller_session_id == bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID));
        if context_route && matches!(action, Some("export_context" | "read_context")) {
            if args.as_object().is_some_and(|fields| {
                fields.keys().any(|key| {
                    !matches!(key.as_str(), "action" | "session_id")
                        && (action == Some("export_context")
                            || !matches!(key.as_str(), "revision" | "file" | "offset" | "limit"))
                })
            }) {
                return Err(ToolError::InvalidArguments("scoped context accepts only target, fixed revision/file and bounded offset/limit; identity and paths are runtime-derived".into()));
            }
            let parsed: SessionInspectorArgs = serde_json::from_value(args).map_err(|e| {
                ToolError::InvalidArguments(format!("Invalid scoped context args: {e}"))
            })?;
            let result = match parsed {
                SessionInspectorArgs::ExportContext { session_id } => {
                    scoped_context::export(self, &ctx, &session_id).await?
                }
                SessionInspectorArgs::ReadContext {
                    session_id,
                    revision,
                    file,
                    offset,
                    limit,
                } => {
                    scoped_context::read(self, &ctx, &session_id, &revision, &file, offset, limit)
                        .await?
                }
                _ => unreachable!("fixed context actions checked"),
            };
            return Ok(ToolOutcome::Completed(result));
        }
        let scoped_discovery = matches!(
            self.access,
            SessionHistoryAccess::Authorized | SessionHistoryAccess::HostRouted
        ) || (self.access == SessionHistoryAccess::Full
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

        let routed = self.access == SessionHistoryAccess::HostRouted;
        let guarded_self =
            self.access == SessionHistoryAccess::SelfOnly && ctx.executing_caller.is_some();
        let mut legacy_supervisor_history = false;
        if routed || guarded_self || ctx.executing_supervisor_for(caller_session_id).is_some() {
            host_discovery::validate_original_caller(self.storage.as_ref(), &ctx).await?;
        }
        if routed {
            let other_target = match &parsed {
                SessionInspectorArgs::GetMeta { session_id }
                | SessionInspectorArgs::ReadMessages { session_id, .. }
                | SessionInspectorArgs::ReadCompressedCache { session_id, .. }
                | SessionInspectorArgs::ExportContext { session_id } => {
                    session_id != caller_session_id
                }
                _ => false,
            };
            if matches!(&parsed, SessionInspectorArgs::Search { .. }) {
                return Err(ToolError::Execution("session_discovery_bounded_scope: global history search is unavailable; use authorized list/inspect or search_current".into()));
            }
            if other_target {
                if matches!(
                    &parsed,
                    SessionInspectorArgs::GetMeta { .. }
                        | SessionInspectorArgs::ReadMessages { .. }
                ) {
                    if ctx.executing_supervisor_for(caller_session_id).is_none() {
                        return Err(ToolError::Execution("session_discovery_bounded_scope: another Session's raw history/metadata is unavailable; use authorized list/inspect".into()));
                    }
                    host_discovery::require_absent_host_for_legacy_history(
                        self.storage.as_ref(),
                        &ctx,
                    )
                    .await?;
                    legacy_supervisor_history = true;
                } else {
                    return Err(ToolError::Execution("session_discovery_bounded_scope: peer raw history/cache/export is unavailable; use authorized list/inspect".into()));
                }
            }
        }

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

        if (self.access == SessionHistoryAccess::Full || legacy_supervisor_history)
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
                    "Supervisor history supports current-user list/inspect and bounded read_messages/get_meta for its own Session or ordinary Root/Child targets; use session_history_current for current-turn paging".into(),
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
            if routed {
                host_discovery::require_absent_host_for_legacy_history(self.storage.as_ref(), &ctx)
                    .await?;
            }
            return Ok(ToolOutcome::Completed(result));
        }

        let outcome = match parsed {
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
            SessionInspectorArgs::ReadContext { .. } => Err(ToolError::Execution(
                "read_context requires an original executing caller and explicit scoped context surface".into(),
            )),
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
        .map(ToolOutcome::Completed)?;
        if routed || guarded_self {
            // Existing current/self readers retain their own filtering/bounds;
            // do not return a result from a replaced/reset original caller.
            host_discovery::validate_original_caller(self.storage.as_ref(), &ctx).await?;
        }
        Ok(outcome)
    }
}
