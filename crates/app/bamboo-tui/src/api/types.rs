//! HTTP wire types for the TUI client.
//!
//! These structs mirror the Bamboo server's REST responses. Some fields are
//! deserialized for contract fidelity (and so the TUI is robust to responses
//! that include them) but are not yet surfaced in the UI — hence the
//! module-scoped `dead_code` allow. New *logic* dead code elsewhere in the
//! crate still warns, since the blanket crate-level allow was removed.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn default_allow_custom() -> bool {
    true
}

fn default_title_generated() -> bool {
    true
}

// Shared HTTP/SSE wire types live in bamboo-client-core (single source of truth
// across the CLI and TUI front-ends); re-exported here so the rest of the TUI
// can keep referring to `crate::api::types::{AgentEvent, ChatRequest, …}`.
pub use bamboo_client_core::{
    AgentEvent, ChatRequest, ChatResponse, ExecuteRequest, ExecuteResponse, TokenUsage,
};

// ── Command catalog ──

/// One entry from the session-aware `GET /api/v1/commands` catalog.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct CommandItem {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    #[serde(rename = "type")]
    pub command_type: String,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommandListResponse {
    #[serde(default)]
    pub commands: Vec<CommandItem>,
    #[serde(default)]
    pub total: usize,
}

/// The content-bearing subset returned by prompt/workflow command resolution.
/// Skill and MCP entries deliberately never use this endpoint in the TUI.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommandDetail {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub content: String,
    #[serde(rename = "type", default)]
    pub command_type: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

// ── Sessions ──

/// Subset of the server's `SessionSummary` (see
/// `bamboo-server/src/handlers/agent/sessions/types.rs`) the TUI renders.
/// Every field carries `#[serde(default)]` so server-side additions/removals
/// degrade gracefully instead of breaking deserialization (there is
/// deliberately no flat `status` string on the server — see `last_run_status`
/// / `is_running` / `has_pending_question` below).
#[derive(Deserialize, Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    /// Stable Project membership used when authoring a schedule from the
    /// currently selected session.
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub title: String,
    #[serde(default = "default_title_generated")]
    pub title_generated: bool,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub model_ref: Option<CatalogModelRef>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub is_running: bool,
    #[serde(default)]
    pub has_pending_question: bool,
    /// Child agents that are still running after the parent run has emitted
    /// its terminal event. The TUI keeps background monitoring alive until
    /// this reaches zero.
    #[serde(default)]
    pub running_child_count: u32,
    #[serde(default)]
    pub last_run_status: Option<String>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub message_count: usize,
    #[serde(default)]
    pub pinned: bool,
    /// Requested per-session permission posture. This must stay visible even
    /// when no permission dialog is open.
    #[serde(default)]
    pub permission_mode: SessionPermissionMode,
    /// Legacy/effective mirror retained by the server. A true value is always
    /// rendered as bypass even if an older server omitted `permission_mode`.
    #[serde(default)]
    pub bypass_permissions: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionPermissionMode {
    #[default]
    Default,
    Bypass,
    Auto,
}

impl SessionPermissionMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Bypass => "bypass",
            Self::Auto => "auto",
        }
    }
}

/// Wire shape of `GET /api/v1/sessions` — the server wraps the page in an
/// envelope (`ListSessionsResponse`, #421/#252) so the list can be paginated
/// instead of growing without bound as session count grows forever.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ListSessionsEnvelope {
    #[serde(default)]
    pub sessions: Vec<SessionSummary>,
    #[serde(default)]
    pub total: usize,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub next_offset: Option<usize>,
}

#[derive(Serialize)]
pub struct RespondRequest {
    pub response: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_tool_call_id: Option<String>,
}

/// Wire shape of `GET /api/v1/sessions/{session_id}` — the server wraps the
/// single summary in a `{ "session": ... }` envelope rather than a bare
/// object.
#[derive(Deserialize, Debug, Clone)]
pub struct GetSessionEnvelope {
    pub session: SessionSummary,
}

/// Full session projection used by the Sub-agents inspector.  The ordinary
/// session picker intentionally keeps a smaller DTO; the tree needs the
/// durable relationship and placement metadata already emitted by the same
/// server endpoints.
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionTreeKind {
    Root,
    Child,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionTreePlacement {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub host: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SessionTreeSummary {
    pub id: String,
    #[serde(default)]
    pub kind: SessionTreeKind,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub root_session_id: String,
    #[serde(default)]
    pub spawn_depth: u32,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub is_running: bool,
    #[serde(default)]
    pub has_pending_question: bool,
    #[serde(default)]
    pub running_child_count: u32,
    #[serde(default)]
    pub last_run_status: Option<String>,
    #[serde(default)]
    pub last_run_error: Option<String>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_activity_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub subagent_type: Option<String>,
    #[serde(default)]
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub resident_name: Option<String>,
    #[serde(default)]
    pub placement: SessionTreePlacement,
}

impl SessionTreeSummary {
    pub(crate) fn placeholder(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: SessionTreeKind::Unknown,
            title: String::new(),
            parent_session_id: None,
            root_session_id: String::new(),
            spawn_depth: 0,
            model: String::new(),
            is_running: false,
            has_pending_question: false,
            running_child_count: 0,
            last_run_status: None,
            last_run_error: None,
            updated_at: None,
            last_activity_at: None,
            subagent_type: None,
            lifecycle: None,
            resident_name: None,
            placement: SessionTreePlacement::default(),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ListSessionTreeEnvelope {
    #[serde(default)]
    pub sessions: Vec<SessionTreeSummary>,
    #[serde(default)]
    pub total: usize,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub next_offset: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GetSessionTreeEnvelope {
    pub session: SessionTreeSummary,
}

// ── Session resume (history + pending question) ──

/// `function` payload of a [`HistoryToolCall`] — mirrors the server's
/// `FunctionCall` (`bamboo-domain::session::tool_types::FunctionCall`).
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryFunctionCall {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

/// One tool call attached to an assistant [`HistoryMessage`] — mirrors the
/// server's `ToolCall`.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryToolCall {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub function: HistoryFunctionCall,
}

/// One entry in `GET /api/v1/history/{session_id}`'s `messages` array — a
/// lenient subset of the server's `Message`
/// (`bamboo-domain::session::types::Message`). Only the fields
/// `history::map_history` needs are modeled; everything else (content_parts,
/// image_ocr, phase, compression fields) is ignored on decode rather than
/// breaking deserialization. Metadata is retained leniently because some
/// servers persist structured child-session summaries there.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryMessage {
    #[serde(default)]
    pub id: String,
    /// "system" | "user" | "assistant" | "tool", lowercase.
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<HistoryToolCall>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_success: Option<bool>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    /// Leniently retained so structured UI rows (notably persisted child
    /// lifecycle summaries) can be reconstructed when a server includes them.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// Wire shape of `GET /api/v1/history/{session_id}` (see the route's ACTUAL
/// registration in `routes/agent.rs` — the doc comment on the handler itself
/// names a different, stale path).
#[derive(Deserialize, Debug, Clone, Default)]
pub struct HistoryResponse {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub messages: Vec<HistoryMessage>,
    #[serde(default)]
    pub is_delta: bool,
    /// Whether the server dropped older messages to stay under its cold-fetch
    /// cap — surfaced to the operator as "showing last N of M messages".
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub total_message_count: usize,
}

/// Wire shape of `GET /api/v1/respond/{session_id}/pending`.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct PendingQuestion {
    #[serde(default)]
    pub has_pending_question: bool,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub options: Option<Vec<String>>,
    #[serde(default = "default_allow_custom")]
    pub allow_custom: bool,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    /// Explicit server classification. A permission interaction with a missing
    /// contract is rendered fail-closed; it is never downgraded to a legacy
    /// English clarification answer.
    #[serde(default)]
    pub interaction_kind: Option<PendingInteractionKind>,
    /// Authoritative typed permission contract, present only when this pending
    /// question is the permission request with the same tool-call/request id.
    #[serde(default)]
    pub permission_request: Option<PermissionRequest>,
    /// Exact originating tool arguments recovered by the server. Kept separate
    /// from `PermissionRequest` because policy evaluation intentionally stores a
    /// bounded resource rather than arbitrary argument payloads.
    #[serde(default)]
    pub tool_arguments: Option<serde_json::Value>,
    /// True when the server bounded a large raw argument payload before
    /// returning it. The exact authorization resource remains available on
    /// `permission_request.resource`; this flag prevents the preview from
    /// being mistaken for the complete invocation.
    #[serde(default)]
    pub tool_arguments_truncated: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PendingInteractionKind {
    Clarification,
    Permission,
}

// ── Typed permissions ──

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecisionKind {
    AllowOnce,
    AllowSession,
    AllowWorkspace,
    AllowGlobal,
    DenyOnce,
    DenySession,
}

impl PermissionDecisionKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::AllowOnce => "Allow once",
            Self::AllowSession => "Allow for session",
            Self::AllowWorkspace => "Allow for workspace",
            Self::AllowGlobal => "Allow globally",
            Self::DenyOnce => "Deny once",
            Self::DenySession => "Deny for session",
        }
    }

    pub fn remembers(self) -> bool {
        matches!(
            self,
            Self::AllowSession | Self::AllowWorkspace | Self::AllowGlobal | Self::DenySession
        )
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionType {
    WriteFile,
    ExecuteCommand,
    GitWrite,
    HttpRequest,
    DeleteOperation,
    TerminalSession,
}

impl PermissionType {
    pub fn label(self) -> &'static str {
        match self {
            Self::WriteFile => "write_file",
            Self::ExecuteCommand => "execute_command",
            Self::GitWrite => "git_write",
            Self::HttpRequest => "http_request",
            Self::DeleteOperation => "delete_operation",
            Self::TerminalSession => "terminal_session",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

impl RiskLevel {
    pub fn label(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionReasonCode {
    PlatformHardDeny,
    HardDangerous,
    ConfiguredAlwaysAsk,
    ExplicitDeny,
    ModeDenied,
    RiskThreshold,
}

impl PermissionReasonCode {
    pub fn label(self) -> &'static str {
        match self {
            Self::PlatformHardDeny => "platform hard deny",
            Self::HardDangerous => "hard-dangerous operation",
            Self::ConfiguredAlwaysAsk => "configured always-ask rule",
            Self::ExplicitDeny => "explicit deny rule",
            Self::ModeDenied => "permission mode restriction",
            Self::RiskThreshold => "risk threshold",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EffectivePermissionMode {
    #[default]
    Default,
    Plan,
    AcceptEdits,
    DontAsk,
    BypassPermissions,
    Auto,
}

impl EffectivePermissionMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
            Self::AcceptEdits => "accept_edits",
            Self::DontAsk => "dont_ask",
            Self::BypassPermissions => "bypass",
            Self::Auto => "auto",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMatcherKind {
    ExactResource,
    PathSubtree,
    CommandPrefix,
    HttpOrigin,
    ToolAction,
}

impl PermissionMatcherKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::ExactResource => "exact resource",
            Self::PathSubtree => "path subtree",
            Self::CommandPrefix => "command prefix",
            Self::HttpOrigin => "HTTP origin",
            Self::ToolAction => "tool action",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct PermissionMatcher {
    pub id: String,
    pub kind: PermissionMatcherKind,
    pub value: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionRuleEffect {
    Allow,
    Deny,
    AlwaysAsk,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionRuleScope {
    Workspace,
    Global,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionRuleSource {
    User,
    Legacy,
    Platform,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct PermissionRuleRef {
    pub id: String,
    pub effect: PermissionRuleEffect,
    pub scope: PermissionRuleScope,
    pub source: PermissionRuleSource,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct PermissionRequest {
    pub request_id: String,
    pub request_generation: String,
    pub session_id: String,
    #[serde(default)]
    pub workspace_path: Option<String>,
    pub tool_name: String,
    pub permission_type: PermissionType,
    pub resource: String,
    pub operation_summary: String,
    pub risk_level: RiskLevel,
    pub reason_code: PermissionReasonCode,
    pub effective_mode: EffectivePermissionMode,
    pub bypass_requested: bool,
    #[serde(default)]
    pub auto_approve_requested: bool,
    pub policy_revision: u64,
    #[serde(default)]
    pub matched_rule: Option<PermissionRuleRef>,
    #[serde(default)]
    pub allowed_decisions: Vec<PermissionDecisionKind>,
    #[serde(default)]
    pub suggested_matchers: Vec<PermissionMatcher>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PermissionDecision {
    pub request_id: String,
    pub request_generation: String,
    pub decision: PermissionDecisionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_policy_revision: Option<u64>,
    #[serde(default)]
    pub confirm_global: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PermissionDecisionResponse {
    pub success: bool,
    #[serde(default)]
    pub replayed: bool,
    #[serde(default)]
    pub auto_resume_status: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DurablePermissionRule {
    pub id: String,
    pub permission_type: PermissionType,
    pub effect: PermissionRuleEffect,
    pub scope: PermissionRuleScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    pub matcher: PermissionMatcher,
    #[serde(default = "default_permission_rule_source")]
    pub source: PermissionRuleSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

fn default_permission_rule_source() -> PermissionRuleSource {
    PermissionRuleSource::User
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct PermissionPolicyConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub mode: Option<EffectivePermissionMode>,
    #[serde(default)]
    pub confirm_threshold: Option<RiskLevel>,
    #[serde(default)]
    pub ask_rules: Vec<String>,
    #[serde(default)]
    pub durable_rules: Vec<DurablePermissionRule>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TemporaryPermissionGrant {
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub effect: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    pub permission_type: PermissionType,
    #[serde(default)]
    pub matcher: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PermissionPolicyResponse {
    pub revision: u64,
    pub policy: PermissionPolicyConfig,
    #[serde(default)]
    pub temporary_grants: Vec<TemporaryPermissionGrant>,
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct PutPermissionRuleRequest {
    pub expected_revision: u64,
    pub rule: DurablePermissionRule,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiagnosePermissionRequest {
    #[serde(default)]
    pub request_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    pub tool_name: String,
    #[serde(default)]
    pub tool_args: serde_json::Value,
    pub permission_type: PermissionType,
    pub resource: String,
    #[serde(default)]
    pub operation_summary: String,
    #[serde(default)]
    pub bypass_requested: bool,
    #[serde(default)]
    pub auto_approve_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_hard_deny: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ChildApprovalDecision {
    pub parent_session_id: String,
    pub child_attempt: u32,
    pub request_id: String,
    pub expected_version: u64,
    pub approved: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChildApprovalResponse {
    pub delivered: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildApprovalState {
    Pending,
    DecisionRecorded,
    Delivered,
    DeliveryFailed,
    Expired,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ChildApprovalRecord {
    pub parent_session_id: String,
    pub child_session_id: String,
    #[serde(default)]
    pub child_attempt: u32,
    pub request_id: String,
    pub tool_name: String,
    pub permission: String,
    pub resource: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub version: u64,
    pub state: ChildApprovalState,
    #[serde(default)]
    pub approved: Option<bool>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SubagentSnapshotResponse {
    pub schema_version: u32,
    pub snapshot_seq: u64,
    pub approvals_revision: u64,
    #[serde(default)]
    pub approvals: Vec<ChildApprovalRecord>,
}

#[derive(Serialize, Debug, Clone)]
pub struct PatchSessionPermissionModeRequest {
    pub permission_mode: SessionPermissionMode,
}

// ── MCP ──

#[derive(Deserialize, Debug, Clone)]
pub struct McpServer {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub enabled: bool,
    #[serde(default)]
    pub transport: serde_json::Value,
    #[serde(default)]
    pub connected: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ToolInfo {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

// ── Schedules ──

/// Flat, display-oriented schedule used by the Schedules tab. Built from the
/// server's richer `ScheduleView` via [`Schedule::from_view`] — the server has
/// no flat `cron`/`prompt` fields, so we project its `trigger`/`state` into the
/// handful of fields the list actually renders.
#[derive(Debug, Clone)]
pub struct Schedule {
    pub id: String,
    pub name: Option<String>,
    pub cron: Option<String>,
    pub enabled: Option<bool>,
    pub prompt: Option<String>,
    pub last_run: Option<DateTime<Utc>>,
    pub next_run: Option<DateTime<Utc>>,
}

impl Schedule {
    pub fn from_view(v: ScheduleView) -> Self {
        Self {
            cron: Some(trigger_display(&v.trigger)),
            name: Some(v.name),
            enabled: Some(v.enabled),
            prompt: v.run_config.task_message,
            last_run: v.state.last_finished_at,
            next_run: v.state.next_fire_at,
            id: v.id,
        }
    }
}

/// Human-readable one-liner for a schedule trigger. Cron triggers show their
/// expression; other trigger kinds show their `type` tag (e.g. `interval`).
fn trigger_display(trigger: &serde_json::Value) -> String {
    match trigger.get("type").and_then(|t| t.as_str()) {
        Some("cron") => trigger
            .get("expr")
            .and_then(|e| e.as_str())
            .unwrap_or("cron")
            .to_string(),
        Some(kind) => kind.to_string(),
        None => "-".to_string(),
    }
}

/// Wire shape of `GET /api/v1/schedules` — the server wraps the list in a
/// `{ "schedules": [...] }` envelope (`ListSchedulesResponse`).
#[derive(Deserialize, Debug, Clone)]
pub struct ListSchedulesResponse {
    #[serde(default)]
    pub schedules: Vec<ScheduleView>,
}

/// Subset of the server's `ScheduleView` the TUI consumes. The full trigger
/// enum is kept as an opaque `Value` (see [`trigger_display`]) so new trigger
/// kinds don't break deserialization.
#[derive(Deserialize, Debug, Clone)]
pub struct ScheduleView {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub trigger: serde_json::Value,
    #[serde(default)]
    pub state: ScheduleStateView,
    #[serde(default)]
    pub run_config: ScheduleRunConfigView,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ScheduleStateView {
    #[serde(default)]
    pub next_fire_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_finished_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ScheduleRunConfigView {
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub task_message: Option<String>,
}

/// Body of `POST /api/v1/schedules`. Must match the server's
/// `CreateScheduleRequest`: a tagged `trigger` and a `run_config` carrying the
/// prompt (there is no flat `cron`/`prompt` on the server side).
#[derive(Serialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub trigger: ScheduleTriggerReq,
    pub enabled: bool,
    pub run_config: ScheduleRunConfigReq,
}

/// Serialize-side mirror of the server's internally-tagged `ScheduleTrigger`.
/// The TUI form only authors cron triggers today.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleTriggerReq {
    Cron { expr: String },
}

#[derive(Serialize, Default)]
pub struct ScheduleRunConfigReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_message: Option<String>,
    /// Run the authored prompt when the schedule fires (only meaningful with a
    /// `task_message`).
    pub auto_execute: bool,
}

// ── Provider catalog (model picker, Ctrl+O) ──

/// Mirrors the server's `ProviderModelRef` (`crates/core/bamboo-domain`) on
/// the wire: `{"provider": "...", "model": "..."}`. The TUI keeps those
/// fields separate on Chat/Execute/PATCH requests so same-named models from
/// different providers remain unambiguous without inventing a combined id.
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogModelRef {
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
}

/// One entry in `GET /v1/bamboo/provider-catalog`'s `models` array — a
/// lenient subset of the server's `ProviderModelDescriptor`
/// (`bamboo-domain::provider_catalog`). `capabilities`/`source`/
/// `discovered_at` aren't rendered by the picker, so they're ignored on
/// decode rather than modeled.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct CatalogModel {
    #[serde(default)]
    pub reference: CatalogModelRef,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub provider_display_name: String,
}

/// Wire shape of `GET /v1/bamboo/provider-catalog`. `providers` isn't
/// rendered by the picker (each model entry already carries
/// `provider_display_name`), so only `models` is modeled.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ProviderCatalog {
    #[serde(default)]
    pub models: Vec<CatalogModel>,
}

/// Body of `PATCH /api/v1/sessions/{id}` for the model-picker's exact
/// provider/model update. The server derives and persists its typed
/// `model_ref` when both legacy-compatible fields are present.
#[derive(Serialize, Debug, Clone)]
pub struct PatchSessionModelRequest {
    pub model: String,
    pub provider: String,
}

/// Canonical metadata subset used by the session picker's rename/pin actions.
/// Both fields are optional so one PATCH can express exactly one deliberate UI
/// mutation while the `If-Match` header protects it from stale overwrites.
#[derive(Serialize, Debug, Clone, Default)]
pub struct PatchSessionMetadataRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
}

// ── Skills ──

#[derive(Deserialize, Debug, Clone)]
pub struct Skill {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SkillDetail {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub tools: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture mirroring the real `ListSessionsResponse` shape (#421), including
    /// fields the TUI doesn't model (`kind`, `placement`, …) — those must be
    /// ignored rather than break deserialization.
    const ENVELOPE_JSON: &str = r#"{
        "sessions": [
            {
                "id": "s1",
                "kind": "root",
                "title": "Fix the bug",
                "title_version": 3,
                "pinned": true,
                "root_session_id": "s1",
                "spawn_depth": 0,
                "model": "claude-sonnet-5",
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-09T12:34:00Z",
                "last_activity_at": "2026-07-09T12:34:00Z",
                "message_count": 12,
                "has_attachments": false,
                "is_running": true,
                "has_pending_question": false,
                "running_child_count": 2,
                "placement": {"kind": "local", "host": "box"}
            },
            {
                "id": "s2",
                "kind": "root",
                "title": "",
                "title_version": 0,
                "pinned": false,
                "root_session_id": "s2",
                "spawn_depth": 0,
                "model": "gpt-5",
                "created_at": "2026-07-01T00:00:00Z",
                "updated_at": "2026-07-08T08:00:00Z",
                "last_activity_at": "2026-07-08T08:00:00Z",
                "message_count": 0,
                "has_attachments": false,
                "is_running": false,
                "has_pending_question": true,
                "last_run_status": "error",
                "running_child_count": 0,
                "placement": {"kind": "local", "host": "box"}
            }
        ],
        "total": 5,
        "limit": 2,
        "offset": 0,
        "next_offset": 2
    }"#;

    #[test]
    fn envelope_deserializes_with_pagination_metadata() {
        let envelope: ListSessionsEnvelope = serde_json::from_str(ENVELOPE_JSON).unwrap();
        assert_eq!(envelope.total, 5);
        assert_eq!(envelope.limit, 2);
        assert_eq!(envelope.offset, 0);
        assert_eq!(envelope.next_offset, Some(2));
        assert_eq!(envelope.sessions.len(), 2);

        let s1 = &envelope.sessions[0];
        assert_eq!(s1.id, "s1");
        assert_eq!(s1.title, "Fix the bug");
        assert_eq!(s1.model, "claude-sonnet-5");
        assert!(s1.is_running);
        assert!(!s1.has_pending_question);
        assert_eq!(s1.running_child_count, 2);
        assert_eq!(s1.message_count, 12);
        assert!(s1.pinned);
        assert!(s1.last_run_status.is_none());

        let s2 = &envelope.sessions[1];
        assert!(s2.title.is_empty());
        assert!(!s2.is_running);
        assert!(s2.has_pending_question);
        assert_eq!(s2.last_run_status.as_deref(), Some("error"));
    }

    /// A last page (`next_offset` omitted entirely) must deserialize to `None`,
    /// and unknown top-level fields must not break parsing.
    #[test]
    fn envelope_tolerates_missing_next_offset_and_unknown_fields() {
        let json = r#"{
            "sessions": [],
            "total": 0,
            "limit": 200,
            "offset": 0,
            "some_future_field": "ignored"
        }"#;
        let envelope: ListSessionsEnvelope = serde_json::from_str(json).unwrap();
        assert!(envelope.sessions.is_empty());
        assert_eq!(envelope.next_offset, None);
    }

    /// A minimal (nearly-empty) session object must still deserialize thanks to
    /// `#[serde(default)]` on every non-id field — the lenient-degrade contract.
    #[test]
    fn session_summary_defaults_missing_fields() {
        let json = r#"{"id": "bare"}"#;
        let s: SessionSummary = serde_json::from_str(json).unwrap();
        assert_eq!(s.id, "bare");
        assert_eq!(s.title, "");
        assert_eq!(s.model, "");
        assert!(!s.is_running);
        assert!(!s.has_pending_question);
        assert!(s.last_run_status.is_none());
        assert!(s.updated_at.is_none());
        assert_eq!(s.message_count, 0);
        assert!(!s.pinned);
        assert_eq!(s.permission_mode, SessionPermissionMode::Default);
        assert!(!s.bypass_permissions);
    }

    #[test]
    fn child_tree_projection_preserves_relationship_and_placement_metadata() {
        let json = r#"{
            "id":"child-2",
            "kind":"child",
            "title":"Review",
            "parent_session_id":"child-1",
            "root_session_id":"root",
            "spawn_depth":2,
            "model":"gpt-5",
            "is_running":true,
            "last_run_error":"old error",
            "subagent_type":"reviewer",
            "lifecycle":"resident",
            "resident_name":"reviewer-a",
            "placement":{"kind":"ssh","host":"worker.example"}
        }"#;
        let session: SessionTreeSummary = serde_json::from_str(json).unwrap();
        assert_eq!(session.kind, SessionTreeKind::Child);
        assert_eq!(session.parent_session_id.as_deref(), Some("child-1"));
        assert_eq!(session.root_session_id, "root");
        assert_eq!(session.spawn_depth, 2);
        assert_eq!(session.subagent_type.as_deref(), Some("reviewer"));
        assert_eq!(session.lifecycle.as_deref(), Some("resident"));
        assert_eq!(session.resident_name.as_deref(), Some("reviewer-a"));
        assert_eq!(session.placement.kind, "ssh");
        assert_eq!(session.placement.host, "worker.example");
    }

    #[test]
    fn child_tree_projection_tolerates_missing_legacy_metadata() {
        let session: SessionTreeSummary = serde_json::from_str(r#"{"id":"legacy"}"#).unwrap();
        assert_eq!(session.kind, SessionTreeKind::Unknown);
        assert!(session.parent_session_id.is_none());
        assert!(session.root_session_id.is_empty());
        assert!(session.placement.host.is_empty());
    }

    #[test]
    fn session_summary_preserves_typed_permission_posture() {
        let bypass: SessionSummary = serde_json::from_str(
            r#"{"id":"bypass","permission_mode":"bypass","bypass_permissions":true}"#,
        )
        .unwrap();
        assert_eq!(bypass.permission_mode, SessionPermissionMode::Bypass);
        assert!(bypass.bypass_permissions);

        let auto: SessionSummary = serde_json::from_str(
            r#"{"id":"auto","permission_mode":"auto","bypass_permissions":false}"#,
        )
        .unwrap();
        assert_eq!(auto.permission_mode, SessionPermissionMode::Auto);
        assert!(!auto.bypass_permissions);
    }

    /// Fixture mirroring the real `GET /api/v1/history/{id}` response,
    /// including server fields the TUI doesn't model (`compression_events`,
    /// `gold_config`, `goal_state`) — those must be ignored, not break
    /// deserialization.
    #[test]
    fn history_response_deserializes_lenient_message_shapes() {
        let json = r#"{
            "session_id": "s1",
            "messages": [
                {"id": "m1", "role": "system", "content": "you are helpful", "created_at": "2026-07-01T00:00:00Z"},
                {"id": "m2", "role": "user", "content": "read foo.txt", "created_at": "2026-07-01T00:00:01Z"},
                {
                    "id": "m3",
                    "role": "assistant",
                    "content": "",
                    "reasoning": "let me check",
                    "tool_calls": [
                        {"id": "t1", "type": "function", "function": {"name": "Read", "arguments": "{\"path\":\"foo.txt\"}"}}
                    ],
                    "created_at": "2026-07-01T00:00:02Z"
                },
                {"id": "m4", "role": "tool", "content": "file body", "tool_call_id": "t1", "tool_success": true, "created_at": "2026-07-01T00:00:03Z"}
            ],
            "is_delta": false,
            "truncated": true,
            "total_message_count": 4,
            "compression_events": [],
            "gold_config": {"anything": "opaque"},
            "goal_state": {"status": "in_progress"}
        }"#;
        let resp: HistoryResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.session_id, "s1");
        assert_eq!(resp.messages.len(), 4);
        assert!(resp.truncated);
        assert_eq!(resp.total_message_count, 4);

        let asst = &resp.messages[2];
        assert_eq!(asst.role, "assistant");
        assert_eq!(asst.reasoning.as_deref(), Some("let me check"));
        let tool_calls = asst.tool_calls.as_ref().unwrap();
        assert_eq!(tool_calls[0].id, "t1");
        assert_eq!(tool_calls[0].function.name, "Read");

        let tool_msg = &resp.messages[3];
        assert_eq!(tool_msg.tool_call_id.as_deref(), Some("t1"));
        assert_eq!(tool_msg.tool_success, Some(true));
    }

    /// A near-empty message (only `role`/`content`) must still deserialize —
    /// the lenient-degrade contract that lets the TUI survive server-side
    /// field additions/removals.
    #[test]
    fn history_message_defaults_missing_fields() {
        let json = r#"{"role": "user", "content": "hi"}"#;
        let m: HistoryMessage = serde_json::from_str(json).unwrap();
        assert_eq!(m.role, "user");
        assert_eq!(m.content, "hi");
        assert!(m.id.is_empty());
        assert!(m.reasoning.is_none());
        assert!(m.tool_calls.is_none());
        assert!(m.tool_call_id.is_none());
        assert!(m.tool_success.is_none());
    }

    /// `has_pending_question: false` responses omit every other field.
    #[test]
    fn pending_question_deserializes_both_shapes() {
        let none: PendingQuestion =
            serde_json::from_str(r#"{"has_pending_question": false}"#).unwrap();
        assert!(!none.has_pending_question);
        assert!(none.question.is_empty());

        let some: PendingQuestion = serde_json::from_str(
            r#"{
                "has_pending_question": true,
                "question": "Run rm -rf?",
                "options": ["Yes", "No"],
                "allow_custom": true,
                "tool_call_id": "t1",
                "tool_name": "Bash",
                "source": "permission"
            }"#,
        )
        .unwrap();
        assert!(some.has_pending_question);
        assert_eq!(some.question, "Run rm -rf?");
        assert_eq!(
            some.options.as_deref(),
            Some(&["Yes".to_string(), "No".to_string()][..])
        );
        assert!(some.allow_custom);
        assert_eq!(
            some.interaction_kind, None,
            "missing discriminator must remain unknown instead of defaulting to clarification"
        );

        let legacy: PendingQuestion =
            serde_json::from_str(r#"{"has_pending_question":true,"question":"Legacy question"}"#)
                .unwrap();
        assert!(
            legacy.allow_custom,
            "missing legacy field preserves free-text compatibility"
        );
    }

    #[test]
    fn pending_question_preserves_typed_permission_and_exact_arguments() {
        let pending: PendingQuestion = serde_json::from_str(
            r#"{
                "has_pending_question": true,
                "question": "Allow git push?",
                "options": ["Approve", "Deny"],
                "allow_custom": false,
                "tool_call_id": "permission-17",
                "tool_name": "Bash",
                "source": "pause_tool",
                "interaction_kind": "permission",
                "permission_request": {
                    "request_id": "permission-17",
                    "request_generation": "generation-17",
                    "session_id": "session-9",
                    "workspace_path": "/workspace/repo",
                    "tool_name": "Bash",
                    "permission_type": "execute_command",
                    "resource": "git push origin dev",
                    "operation_summary": "Push the dev branch",
                    "risk_level": "high",
                    "reason_code": "configured_always_ask",
                    "effective_mode": "bypassPermissions",
                    "bypass_requested": true,
                    "auto_approve_requested": false,
                    "policy_revision": 41,
                    "matched_rule": {
                        "id": "always-ask-git-push",
                        "effect": "always_ask",
                        "scope": "global",
                        "source": "user"
                    },
                    "allowed_decisions": ["allow_once", "deny_once"],
                    "suggested_matchers": [{
                        "id": "exact_resource",
                        "kind": "exact_resource",
                        "value": "git push origin dev"
                    }]
                },
                "tool_arguments": {
                    "command": "git push origin dev",
                    "cwd": "/workspace/repo"
                },
                "tool_arguments_truncated": false
            }"#,
        )
        .unwrap();

        let request = pending
            .permission_request
            .as_ref()
            .expect("typed request must survive pending response decoding");
        assert_eq!(request.request_id, "permission-17");
        assert_eq!(request.request_generation, "generation-17");
        assert_eq!(request.session_id, "session-9");
        assert_eq!(request.permission_type, PermissionType::ExecuteCommand);
        assert_eq!(request.risk_level, RiskLevel::High);
        assert_eq!(
            request.reason_code,
            PermissionReasonCode::ConfiguredAlwaysAsk
        );
        assert_eq!(
            request.effective_mode,
            EffectivePermissionMode::BypassPermissions
        );
        assert!(request.bypass_requested);
        assert_eq!(request.policy_revision, 41);
        assert_eq!(
            request.allowed_decisions,
            [
                PermissionDecisionKind::AllowOnce,
                PermissionDecisionKind::DenyOnce
            ]
        );
        assert_eq!(request.suggested_matchers[0].id, "exact_resource");
        assert_eq!(
            pending.interaction_kind,
            Some(PendingInteractionKind::Permission)
        );
        assert!(!pending.tool_arguments_truncated);
        assert_eq!(
            pending.tool_arguments,
            Some(serde_json::json!({
                "command": "git push origin dev",
                "cwd": "/workspace/repo"
            }))
        );
    }

    #[test]
    fn respond_request_serializes_optional_question_identity() {
        let guarded = serde_json::to_value(RespondRequest {
            response: "Yes".to_string(),
            expected_tool_call_id: Some("t1".to_string()),
        })
        .unwrap();
        assert_eq!(guarded["expected_tool_call_id"], "t1");

        let legacy = serde_json::to_value(RespondRequest {
            response: "Yes".to_string(),
            expected_tool_call_id: None,
        })
        .unwrap();
        assert!(legacy.get("expected_tool_call_id").is_none());
    }

    #[test]
    fn permission_decisions_preserve_scope_identity_and_confirmation_fields() {
        let cases = [
            (PermissionDecisionKind::AllowOnce, None, None, false),
            (
                PermissionDecisionKind::AllowSession,
                Some("exact_resource"),
                None,
                false,
            ),
            (
                PermissionDecisionKind::AllowWorkspace,
                Some("path_subtree"),
                Some(17),
                false,
            ),
            (
                PermissionDecisionKind::AllowGlobal,
                Some("command_prefix"),
                Some(17),
                true,
            ),
            (PermissionDecisionKind::DenyOnce, None, None, false),
            (
                PermissionDecisionKind::DenySession,
                Some("exact_resource"),
                None,
                false,
            ),
        ];

        for (decision, matcher_id, expected_policy_revision, confirm_global) in cases {
            let value = serde_json::to_value(PermissionDecision {
                request_id: "permission-17".to_string(),
                request_generation: "generation-17".to_string(),
                decision,
                matcher_id: matcher_id.map(str::to_string),
                expected_policy_revision,
                confirm_global,
            })
            .unwrap();

            assert_eq!(value["request_id"], "permission-17");
            assert_eq!(value["request_generation"], "generation-17");
            assert_eq!(
                value["decision"],
                serde_json::to_value(decision).unwrap(),
                "decision kind must remain machine-readable"
            );
            match matcher_id {
                Some(matcher_id) => assert_eq!(value["matcher_id"], matcher_id),
                None => assert!(value.get("matcher_id").is_none()),
            }
            match expected_policy_revision {
                Some(revision) => assert_eq!(value["expected_policy_revision"], revision),
                None => assert!(value.get("expected_policy_revision").is_none()),
            }
            assert_eq!(value["confirm_global"], confirm_global);
            assert_eq!(
                confirm_global,
                decision == PermissionDecisionKind::AllowGlobal
            );
        }
    }

    /// `GET /api/v1/sessions/{id}` wraps the summary in a `{ "session": ... }`
    /// envelope.
    #[test]
    fn get_session_envelope_unwraps() {
        let json = r#"{"session": {"id": "s1", "model": "claude-sonnet-5"}}"#;
        let envelope: GetSessionEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.session.id, "s1");
        assert_eq!(envelope.session.model, "claude-sonnet-5");
    }

    /// Fixture mirroring the real `GET /v1/bamboo/provider-catalog` response
    /// (`bamboo-domain::ProviderCatalog`), including fields the picker doesn't
    /// model (`providers`, `capabilities`, `source`, `updated_at`) — those must
    /// be ignored, not break deserialization.
    #[test]
    fn provider_catalog_deserializes_lenient_model_shapes() {
        let json = r#"{
            "providers": [
                {"id": "openai", "display_name": "OpenAI", "enabled": true, "authenticated": true}
            ],
            "models": [
                {
                    "reference": {"provider": "openai", "model": "gpt-4.1"},
                    "display_name": "GPT-4.1",
                    "provider_display_name": "OpenAI",
                    "capabilities": {"supports_tools": true, "supports_vision": true},
                    "source": "upstream",
                    "discovered_at": "2026-07-01T00:00:00Z"
                },
                {
                    "reference": {"provider": "anthropic", "model": "claude-sonnet-5"},
                    "display_name": "Claude Sonnet 5",
                    "provider_display_name": "Anthropic"
                }
            ],
            "updated_at": "2026-07-09T00:00:00Z"
        }"#;
        let catalog: ProviderCatalog = serde_json::from_str(json).unwrap();
        assert_eq!(catalog.models.len(), 2);
        let m0 = &catalog.models[0];
        assert_eq!(m0.reference.provider, "openai");
        assert_eq!(m0.reference.model, "gpt-4.1");
        assert_eq!(m0.display_name, "GPT-4.1");
        assert_eq!(m0.provider_display_name, "OpenAI");
        let m1 = &catalog.models[1];
        assert_eq!(m1.reference.model, "claude-sonnet-5");
    }

    /// An empty catalog (no providers configured) must still deserialize —
    /// the model picker keys "nothing to show" off `models.is_empty()`.
    #[test]
    fn provider_catalog_tolerates_empty_models() {
        let json = r#"{"providers": [], "models": []}"#;
        let catalog: ProviderCatalog = serde_json::from_str(json).unwrap();
        assert!(catalog.models.is_empty());
    }

    #[test]
    fn patch_session_model_request_serializes_model_field() {
        let req = PatchSessionModelRequest {
            model: "gpt-4.1".to_string(),
            provider: "openai".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"model":"gpt-4.1","provider":"openai"}"#);
    }

    #[test]
    fn patch_session_metadata_omits_unrelated_fields() {
        let rename = PatchSessionMetadataRequest {
            title: Some("Renamed".to_string()),
            pinned: None,
        };
        assert_eq!(
            serde_json::to_string(&rename).unwrap(),
            r#"{"title":"Renamed"}"#
        );

        let pin = PatchSessionMetadataRequest {
            title: None,
            pinned: Some(true),
        };
        assert_eq!(serde_json::to_string(&pin).unwrap(), r#"{"pinned":true}"#);
    }
}
