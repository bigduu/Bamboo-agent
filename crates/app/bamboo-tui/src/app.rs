use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use chrono::{DateTime, Utc};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tui_textarea::TextArea;

use crate::api::sse::{SessionSseEvent, SseStream};
use crate::api::types::*;
use crate::api::{BambooClient, RespondFailure};
use crate::event::AppEvent;
use crate::history::map_history;
use crate::search::ranked_indices;
use crate::ui;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tab {
    Chat,
    Sessions,
    Mcp,
    Schedules,
    Skills,
    Config,
}

impl Tab {
    pub const ALL: [Tab; 6] = [
        Tab::Chat,
        Tab::Sessions,
        Tab::Mcp,
        Tab::Schedules,
        Tab::Skills,
        Tab::Config,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Tab::Chat => "Chat",
            Tab::Sessions => "Sessions",
            Tab::Mcp => "MCP",
            Tab::Schedules => "Schedules",
            Tab::Skills => "Skills",
            Tab::Config => "Config",
        }
    }

    fn next(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap_or(0);
        Self::ALL[(idx + 1) % Self::ALL.len()]
    }

    fn prev(self) -> Tab {
        let idx = Self::ALL.iter().position(|&t| t == self).unwrap_or(0);
        if idx == 0 {
            Self::ALL[Self::ALL.len() - 1]
        } else {
            Self::ALL[idx - 1]
        }
    }

    fn from_index(i: usize) -> Option<Tab> {
        Self::ALL.get(i).copied()
    }
}

// ── Chat state ──

#[derive(Debug, Clone)]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone)]
pub struct ToolCallDisplay {
    /// `tool_call_id` from the SSE stream. Tool events (Complete/Error/
    /// Lifecycle) are paired by this id, not list position — with parallel
    /// tool calls, position-based pairing lands results on the wrong entry.
    pub id: String,
    pub tool_name: String,
    pub arguments: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub phase: String,
}

#[derive(Debug, Clone)]
pub struct SubAgentDisplay {
    pub child_session_id: String,
    pub title: Option<String>,
    /// "running" | "completed" | "cancelled" | "error" | "skipped".
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: String,
    pub tool_calls: Vec<ToolCallDisplay>,
    pub reasoning: Option<String>,
}

/// Textarea placeholder shown on an empty Chat input, kept as one constant so
/// the initial state and the post-send reset (`handle_chat_key`) can't drift.
const CHAT_PLACEHOLDER: &str = "Type a message... (Enter send · Alt+Enter newline)";

pub struct ChatState {
    pub session_id: Option<String>,
    /// Project inherited from the opened/selected session and propagated when
    /// Ctrl+N creates a new root session.
    pub project_id: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub textarea: TextArea<'static>,
    pub scroll_offset: u16,
    /// Largest meaningful `scroll_offset` for the transcript as last rendered
    /// (`total_lines - visible_height`), refreshed every frame by
    /// `ui::chat::render`. `App` only has `&App` at render time, so this is a
    /// `Cell` — it lets the render function record the bound through a shared
    /// reference instead of needing `&mut App` there. Key/mouse handlers clamp
    /// against `.get()` so scrolling past the bottom can't require an equal
    /// number of up-presses to "catch up" before the view visibly moves.
    pub max_scroll: Cell<u16>,
    pub auto_scroll: bool,
    pub streaming: bool,
    pub current_response: String,
    pub current_tool_calls: Vec<ToolCallDisplay>,
    pub current_reasoning: String,
    pub model: String,
    /// Provider paired with `model` when the selection has an authoritative
    /// catalog/session identity. Kept across Ctrl+N so a new session does not
    /// silently fall back to another provider for a same-named model.
    pub provider: Option<String>,
    pub token_usage: Option<TokenUsage>,
    pub plan_mode: bool,
    /// When true, tool-call arguments and results render in full instead
    /// of truncated. Toggled with `x` on the Chat tab.
    pub expand_tools: bool,
    /// Sub-agents spawned by the current run (child lifecycle).
    pub sub_agents: Vec<SubAgentDisplay>,
}

impl ChatState {
    pub fn new() -> Self {
        let mut textarea = TextArea::default();
        textarea.set_placeholder_text(CHAT_PLACEHOLDER);
        Self {
            session_id: None,
            project_id: None,
            messages: Vec::new(),
            textarea,
            scroll_offset: 0,
            max_scroll: Cell::new(0),
            auto_scroll: true,
            streaming: false,
            current_response: String::new(),
            current_tool_calls: Vec::new(),
            current_reasoning: String::new(),
            model: String::new(),
            provider: None,
            token_usage: None,
            plan_mode: false,
            expand_tools: false,
            sub_agents: Vec::new(),
        }
    }
}

/// Result of a session resume (history + summary + pending question fetched
/// off the event loop), posted back as `AppEvent::SessionOpened`. Kept
/// separate from `ChatState` since it's a one-shot transfer object, not
/// long-lived UI state.
pub struct OpenedSession {
    pub messages: Vec<ChatMessage>,
    pub model: String,
    pub provider: Option<String>,
    pub project_id: Option<String>,
    pub is_running: bool,
    pub pending: Option<PendingQuestion>,
    /// The server dropped older messages to stay under its cold-fetch cap.
    pub truncated: bool,
    /// Pre-cap message count, for the "showing last N of M" notice.
    pub total_message_count: usize,
}

// ── Per-tab states ──

pub struct SessionsState {
    pub sessions: Vec<SessionSummary>,
    pub selected: usize,
    pub loading: bool,
    pub error: Option<String>,
    /// Total sessions across all pages, as reported by the server envelope
    /// (#421). Used to render `page X/Y · total N`.
    pub total: usize,
    /// Offset of the currently-loaded page. Advanced/retreated by `]`/`[`.
    pub offset: usize,
    /// The page size the server actually applied (its clamped default, or the
    /// client-requested value echoed back) — used both to render the page
    /// number and to step `offset` by a full page on `[`. `0` until the first
    /// page loads, at which point `list_sessions` is asked for the server's
    /// own default instead of an arbitrary client guess.
    pub page_limit: usize,
    /// Offset to request for the next page, or `None` on the last page —
    /// gates whether `]` does anything.
    pub next_offset: Option<usize>,
}

impl SessionsState {
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            selected: 0,
            loading: false,
            error: None,
            total: 0,
            offset: 0,
            page_limit: 0,
            next_offset: None,
        }
    }
}

pub struct McpState {
    pub servers: Vec<McpServer>,
    pub selected: usize,
    pub tools: Vec<ToolInfo>,
    pub loading: bool,
    pub error: Option<String>,
}

impl McpState {
    pub fn new() -> Self {
        Self {
            servers: Vec::new(),
            selected: 0,
            tools: Vec::new(),
            loading: false,
            error: None,
        }
    }
}

pub struct SchedulesState {
    pub schedules: Vec<Schedule>,
    pub selected: usize,
    pub loading: bool,
    pub error: Option<String>,
}

impl SchedulesState {
    pub fn new() -> Self {
        Self {
            schedules: Vec::new(),
            selected: 0,
            loading: false,
            error: None,
        }
    }
}

pub struct SkillsState {
    pub skills: Vec<Skill>,
    pub selected: usize,
    pub detail: Option<SkillDetail>,
    pub loading: bool,
    pub error: Option<String>,
}

impl SkillsState {
    pub fn new() -> Self {
        Self {
            skills: Vec::new(),
            selected: 0,
            detail: None,
            loading: false,
            error: None,
        }
    }
}

pub struct ConfigState {
    pub config: Option<serde_json::Value>,
    pub loading: bool,
    pub error: Option<String>,
    pub scroll_offset: u16,
    /// Largest meaningful `scroll_offset` for the raw config view as last
    /// rendered; see `ChatState::max_scroll` for the `Cell`-through-`&App`
    /// rationale.
    pub max_scroll: Cell<u16>,
}

impl ConfigState {
    pub fn new() -> Self {
        Self {
            config: None,
            loading: false,
            error: None,
            scroll_offset: 0,
            max_scroll: Cell::new(0),
        }
    }
}

// ── Notifications ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeLevel {
    Info,
    Warn,
    Error,
}

/// One entry in the notification scrollback (viewed with `Ctrl+L`). Errors and
/// warnings would otherwise be lost when the next status message overwrites the
/// single status line.
#[derive(Debug, Clone)]
pub struct Notification {
    pub level: NoticeLevel,
    pub text: String,
    pub at: DateTime<Utc>,
}

// ── Interactive question (permission gate / clarification) ──

/// An agent question awaiting the operator's answer, driven by the modal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuestionIdentity {
    pub session_id: String,
    pub tool_call_id: Option<String>,
    /// Fallback discriminator for older/external events that do not carry a
    /// tool-call id. The server-side optimistic guard is available whenever
    /// `tool_call_id` is present; this keeps local replay/draft state separate
    /// even for legacy events.
    pub question: String,
}

impl QuestionIdentity {
    fn new(session_id: String, tool_call_id: Option<String>, question: String) -> Self {
        let fallback_question = if tool_call_id.is_none() {
            question
        } else {
            String::new()
        };
        Self {
            session_id,
            tool_call_id,
            question: fallback_question,
        }
    }
}

const MAX_QUESTION_DRAFTS: usize = 64;

/// Draft cache key. Typed questions are keyed by their durable tool-call id;
/// legacy questions additionally retain the visible contract so a later
/// `/pending` hydration cannot carry text onto a different prompt that happens
/// to reuse the same question label.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QuestionDraftKey {
    identity: QuestionIdentity,
    fallback_options: Vec<String>,
    fallback_allow_custom: bool,
}

impl QuestionDraftKey {
    fn new(
        session_id: String,
        tool_call_id: Option<String>,
        question: String,
        options: Vec<String>,
        allow_custom: bool,
    ) -> Self {
        let legacy = tool_call_id.is_none();
        Self {
            identity: QuestionIdentity::new(session_id, tool_call_id, question),
            fallback_options: if legacy { options } else { Vec::new() },
            fallback_allow_custom: legacy && allow_custom,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QuestionOptionHitbox {
    pub x: u16,
    pub y: u16,
    pub width: u16,
    pub index: usize,
}

impl QuestionOptionHitbox {
    fn contains(self, column: u16, row: u16) -> bool {
        row == self.y && column >= self.x && column < self.x.saturating_add(self.width)
    }
}

/// An agent question awaiting the operator's answer, driven by the modal.
pub struct ActiveQuestion {
    pub session_id: String,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
    pub source: Option<String>,
    pub question: String,
    /// Preset choices; empty means free-text only.
    pub options: Vec<String>,
    /// Highlighted option index (option-select mode).
    pub selected: usize,
    pub allow_custom: bool,
    /// `Some(buf)` = free-text entry mode (typing into `buf`); `None` =
    /// option-select mode. Starts `Some("")` when there are no options.
    pub custom: Option<String>,
    /// Draft retained while focus returns to the option list. The currently
    /// edited value lives in `custom`; `custom_draft` keeps it when custom mode
    /// is temporarily closed.
    pub custom_draft: String,
    /// Numeric jump entry (`g`, digits, Enter) reaches options beyond 1-9
    /// without submitting an ambiguous/truncated display label.
    pub number_entry: Option<String>,
    /// Full-text inspector for long questions and selected option values.
    pub inspecting: bool,
    /// `false` inspects/copies the question; `true` the selected exact option.
    pub inspect_option: bool,
    pub inspect_scroll: u16,
    /// Last maximum scroll measured by the renderer. Input handling clamps to
    /// this value so a terminal resize cannot leave an unreachable raw offset.
    pub inspect_max_scroll: Cell<u16>,
    /// Last submission error, rendered in the modal until the next retry.
    pub error: Option<String>,
    /// Option rows recorded by the renderer for click hit-testing.
    pub option_hitboxes: RefCell<Vec<QuestionOptionHitbox>>,
    pub mouse_pressed_option: Option<usize>,
    /// An answer POST is in flight for this question: the modal renders a
    /// "Submitting answer…" state and `handle_question_key` swallows every
    /// key (preventing a double-submit on repeated Enter, and preventing the
    /// modal from being dismissed/mutated out from under the request) until
    /// `AppEvent::AnswerSubmitted` lands. Cleared on failure so the operator
    /// can retry; on success the whole question is dropped.
    pub submitting: bool,
    /// A legacy/external event omitted its durable tool-call identity. The
    /// modal remains inspectable but cannot submit until `/pending` supplies
    /// the exact CAS guard.
    pub identity_syncing: bool,
}

impl ActiveQuestion {
    /// Build the modal state from a `GET .../pending` response. Mirrors the
    /// SSE `NeedClarification` handler: no preset options opens straight into
    /// free-text entry instead of an empty option list.
    fn from_pending(session_id: String, pending: &PendingQuestion, draft: String) -> Self {
        let options = pending.options.clone().unwrap_or_default();
        let custom = if options.is_empty() && pending.allow_custom {
            Some(draft.clone())
        } else {
            None
        };
        Self {
            session_id,
            tool_call_id: pending.tool_call_id.clone(),
            tool_name: pending.tool_name.clone(),
            source: pending.source.clone(),
            question: pending.question.clone(),
            options,
            selected: 0,
            allow_custom: pending.allow_custom,
            custom,
            custom_draft: draft,
            number_entry: None,
            inspecting: false,
            inspect_option: false,
            inspect_scroll: 0,
            inspect_max_scroll: Cell::new(0),
            error: None,
            option_hitboxes: RefCell::new(Vec::new()),
            mouse_pressed_option: None,
            submitting: false,
            identity_syncing: pending.tool_call_id.is_none(),
        }
    }

    fn identity(&self) -> QuestionIdentity {
        QuestionIdentity::new(
            self.session_id.clone(),
            self.tool_call_id.clone(),
            self.question.clone(),
        )
    }

    fn draft_key(&self) -> QuestionDraftKey {
        QuestionDraftKey::new(
            self.session_id.clone(),
            self.tool_call_id.clone(),
            self.question.clone(),
            self.options.clone(),
            self.allow_custom,
        )
    }

    /// A legacy live event can omit the durable tool-call id while exposing
    /// the rest of the exact question contract.  Let the authoritative
    /// `/pending` snapshot fill in that one missing identity component in
    /// place so an operator's custom draft survives the handshake.  Contract
    /// mismatches still replace the modal instead of carrying input onto a
    /// different question.
    fn can_hydrate_identity_from(&self, incoming: &Self) -> bool {
        self.tool_call_id.is_none()
            && incoming.tool_call_id.is_some()
            && self.session_id == incoming.session_id
            && self.question == incoming.question
            && self.options == incoming.options
            && self.allow_custom == incoming.allow_custom
    }

    fn refresh_contract(&mut self, incoming: Self) {
        let was_identity_syncing = self.identity_syncing;
        self.tool_call_id = incoming.tool_call_id;
        self.tool_name = incoming.tool_name;
        self.source = incoming.source;
        self.question = incoming.question;
        self.options = incoming.options;
        self.option_hitboxes.borrow_mut().clear();
        self.mouse_pressed_option = None;
        self.allow_custom = incoming.allow_custom;
        self.identity_syncing = incoming.identity_syncing;
        if was_identity_syncing && !self.identity_syncing {
            self.error = None;
        }
        self.selected = self.selected.min(self.options.len().saturating_sub(1));
        if self.options.is_empty() && self.inspect_option {
            self.inspect_option = false;
            self.inspect_scroll = 0;
        }
        if !self.allow_custom {
            if let Some(draft) = self.custom.take() {
                self.custom_draft = draft;
            }
        } else if self.options.is_empty() && self.custom.is_none() {
            self.custom = Some(std::mem::take(&mut self.custom_draft));
        }
    }

    fn draft(&self) -> &str {
        self.custom.as_deref().unwrap_or(&self.custom_draft)
    }
}

/// In-progress "new schedule" form (opened with `n` on the Schedules tab).
#[derive(Default)]
pub struct ScheduleForm {
    pub name: String,
    pub cron: String,
    pub prompt: String,
    /// Focused field: 0 = name, 1 = cron, 2 = prompt.
    pub field: usize,
}

impl ScheduleForm {
    fn current_mut(&mut self) -> &mut String {
        match self.field {
            0 => &mut self.name,
            1 => &mut self.cron,
            _ => &mut self.prompt,
        }
    }
}

/// In-place editor for the raw config JSON (opened with `e` on the Config tab).
/// The buffer is a multi-line `TextArea`; on save it must parse as JSON before
/// it is PATCHed to the server.
pub struct ConfigEditor {
    pub textarea: TextArea<'static>,
}

/// In-progress model picker (`Ctrl+O` on the Chat tab). Opens immediately with
/// `loading: true` and an empty list; the provider-catalog fetch runs off the
/// event loop and lands via `AppEvent::CatalogLoaded`.
pub struct ModelPicker {
    pub epoch: u64,
    pub models: Vec<CatalogModel>,
    pub visible: Vec<usize>,
    pub query: String,
    pub selected: usize,
    pub loading: bool,
    pub applying: bool,
    pub error: Option<String>,
}

impl ModelPicker {
    fn selected_model(&self) -> Option<&CatalogModel> {
        self.visible
            .get(self.selected)
            .and_then(|index| self.models.get(*index))
    }

    fn refresh_filter(&mut self, preserve: Option<(String, String)>) {
        // Keep the catalog's current/recent/provider grouping stable while
        // filtering. `ranked_indices` still supplies fuzzy subsequence
        // matching, but visible rows retain their pre-grouped source order
        // instead of interleaving providers by match score.
        let ranked = ranked_indices(&self.models, &self.query, model_search_text);
        let mut matched = vec![false; self.models.len()];
        for index in ranked {
            if let Some(slot) = matched.get_mut(index) {
                *slot = true;
            }
        }
        self.visible = matched
            .into_iter()
            .enumerate()
            .filter_map(|(index, is_match)| is_match.then_some(index))
            .collect();
        self.selected = preserve
            .and_then(|key| {
                self.visible.iter().position(|index| {
                    self.models
                        .get(*index)
                        .is_some_and(|model| model_key(model) == key)
                })
            })
            .unwrap_or(0)
            .min(self.visible.len().saturating_sub(1));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionPickerIntent {
    Rename,
    Pin(bool),
}

#[derive(Debug)]
pub enum SessionPickerMode {
    Browse,
    Rename {
        session_id: String,
        draft: String,
        /// Title represented by the ETag preparation snapshot. A fresh GET
        /// must reconcile this baseline before its version can authorize the
        /// draft, otherwise a stale list row could silently revert a rename.
        base_title: String,
        draft_dirty: bool,
        metadata_version: Option<u64>,
        loading_version: bool,
        submitting: bool,
        error: Option<String>,
    },
    Pinning {
        session_id: String,
        target: bool,
        loading_version: bool,
        submitting: bool,
        error: Option<String>,
    },
}

impl SessionPickerMode {
    fn matches(&self, session_id: &str, intent: &SessionPickerIntent) -> bool {
        match (self, intent) {
            (Self::Rename { session_id: id, .. }, SessionPickerIntent::Rename) => id == session_id,
            (
                Self::Pinning {
                    session_id: id,
                    target,
                    ..
                },
                SessionPickerIntent::Pin(expected),
            ) => id == session_id && target == expected,
            _ => false,
        }
    }
}

pub struct SessionPicker {
    pub epoch: u64,
    pub sessions: Vec<SessionSummary>,
    pub visible: Vec<usize>,
    pub query: String,
    pub selected: usize,
    /// Once the operator moves or filters the cursor, later lazy pages must
    /// preserve that choice instead of jumping back to the active session.
    pub selection_touched: bool,
    pub loading: bool,
    pub error: Option<String>,
    pub total: usize,
    pub page_limit: usize,
    pub next_offset: Option<usize>,
    pub mode: SessionPickerMode,
}

impl SessionPicker {
    fn selected_session(&self) -> Option<&SessionSummary> {
        self.visible
            .get(self.selected)
            .and_then(|index| self.sessions.get(*index))
    }

    fn refresh_filter(&mut self, preserve_id: Option<String>) {
        self.visible = ranked_indices(&self.sessions, &self.query, session_search_text);
        self.selected = preserve_id
            .and_then(|id| {
                self.visible.iter().position(|index| {
                    self.sessions
                        .get(*index)
                        .is_some_and(|session| session.id == id)
                })
            })
            .unwrap_or(0)
            .min(self.visible.len().saturating_sub(1));
    }
}

const MAX_SESSION_PICKER_SESSIONS: usize = 1_000;
const MAX_RECENT_MODELS: usize = 8;

fn model_key(model: &CatalogModel) -> (String, String) {
    (
        model.reference.provider.clone(),
        model.reference.model.clone(),
    )
}

fn model_search_text(model: &CatalogModel) -> String {
    format!(
        "{} {} {} {}",
        model.display_name,
        model.provider_display_name,
        model.reference.provider,
        model.reference.model
    )
}

fn session_search_text(session: &SessionSummary) -> String {
    let status = if session.is_running {
        "running"
    } else if session.has_pending_question {
        "question awaiting"
    } else {
        session.last_run_status.as_deref().unwrap_or("idle")
    };
    format!(
        "{} {} {} {} {}",
        session.title,
        session.model,
        session.id,
        status,
        if session.pinned { "pinned" } else { "" }
    )
}

fn current_model_key(
    models: &[CatalogModel],
    current_provider: Option<&str>,
    current_model: &str,
) -> Option<(String, String)> {
    if current_model.trim().is_empty() {
        return None;
    }
    if let Some(provider) = current_provider.filter(|provider| !provider.trim().is_empty()) {
        return Some((provider.to_string(), current_model.to_string()));
    }

    // Legacy sessions expose only a bare model id. Treat it as Current only
    // when the catalog maps that id to exactly one provider; duplicate ids are
    // deliberately left unmarked rather than claiming several providers are
    // simultaneously active.
    let mut matches = models
        .iter()
        .filter(|model| model.reference.model == current_model)
        .map(model_key);
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

fn sort_catalog_models(
    models: &mut [CatalogModel],
    current: Option<&(String, String)>,
    recent: &VecDeque<(String, String)>,
) {
    models.sort_by_key(|model| {
        let key = model_key(model);
        let current_rank = usize::from(current != Some(&key));
        let recent_rank = recent
            .iter()
            .position(|candidate| candidate == &key)
            .unwrap_or(usize::MAX);
        (
            current_rank,
            recent_rank,
            model.provider_display_name.to_lowercase(),
            model.display_name.to_lowercase(),
            key,
        )
    });
}

// ── Auto-serve (local `bamboo serve` bootstrap) ──

/// How `App::run`'s startup connectivity check should react when it fails
/// against a loopback URL (see [`is_loopback_url`]); irrelevant for a remote
/// URL, which always just warns regardless of this. Set once from CLI flags
/// (`--auto-serve` / `--no-auto-serve`, mutually exclusive) and consumed by
/// `run` before the main loop starts — nothing later re-reads it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AutoServeMode {
    /// `--auto-serve`: skip the y/n offer and start spawning immediately.
    Auto,
    /// Default: open the [`ServeOffer`] y/n modal.
    Prompt,
    /// `--no-auto-serve`: never offer or spawn; just warn, like a remote URL.
    Off,
}

/// Startup-only "start a local server?" y/n prompt (`AutoServeMode::Prompt`).
/// Exists ONLY before `App::run`'s main loop starts driving any other
/// modal — nothing after startup ever sets it again — which is why it is
/// checked first in `handle_key`'s modal-precedence chain.
pub struct ServeOffer {
    pub url: String,
}

/// The `serve` subcommand's own compiled-in defaults (`bamboo-config`'s
/// `default_port`/`default_bind`). Duplicated here rather than imported —
/// `bamboo-tui` intentionally has no dependency on the server crates (see
/// `Cargo.toml`) — and used only to decide whether the auto-spawned server
/// needs explicit `--port`/`--bind` overrides at all.
const DEFAULT_SERVER_PORT: u16 = 9562;
const DEFAULT_SERVER_BIND: &str = "127.0.0.1";

/// Split an http(s) URL's authority into `(host, port)`. Pure string
/// parsing — no new deps, no DNS resolution. Handles a bracketed IPv6 host
/// (`[::1]:9562`), userinfo (`user:pass@host`), and a missing port (`None`,
/// meaning the server's own default applies).
fn parse_authority(url: &str) -> (String, Option<u16>) {
    let without_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    // Drop any path/query/fragment after the authority.
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(without_scheme);
    // Drop userinfo, if present.
    let authority = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);

    if let Some(rest) = authority.strip_prefix('[') {
        // Bracketed IPv6: `[::1]:9562` or bare `[::1]`.
        let mut parts = rest.splitn(2, ']');
        let host = parts.next().unwrap_or(rest).to_string();
        let port = parts
            .next()
            .and_then(|tail| tail.strip_prefix(':'))
            .and_then(|p| p.parse().ok());
        (host, port)
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.parse().ok()),
            None => (authority.to_string(), None),
        }
    }
}

/// Whether `url`'s host is loopback (`127.0.0.1`/`127.x.x.x`, `localhost`,
/// `::1` — bracketed or not) — i.e. safe to offer auto-starting a local
/// `bamboo serve` for. A hostname other than the literal `localhost` is
/// never resolved/DNS-checked; this is a syntactic check only, matching
/// `parse_authority`'s no-new-deps string parsing. The `127.x.x.x` case
/// parses `host` as a strict `Ipv4Addr` literal (std, no new dep) rather
/// than `starts_with("127.")` — the latter would also match a non-loopback
/// hostname like `127.0.0.1.evil.example.com`, wrongly offering/auto-
/// starting a local server for what the operator meant as a remote URL.
pub fn is_loopback_url(url: &str) -> bool {
    let (host, _) = parse_authority(url);
    host == "localhost"
        || host == "::1"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.octets()[0] == 127)
}

/// `bamboo`'s platform binary name, used both by `discover_bamboo_bin` and
/// its tests.
fn bamboo_bin_name() -> &'static str {
    if cfg!(windows) {
        "bamboo.exe"
    } else {
        "bamboo"
    }
}

/// Resolve the `bamboo` binary to spawn for auto-serve, in precedence order:
/// (a) `$BAMBOO_BIN`, if set — even when the path doesn't exist, so a
///     typo'd override fails loudly at spawn time instead of silently
///     falling through to a different binary; (b) a `bamboo`/`bamboo.exe`
///     binary sitting next to the running `bamboo-tui` executable (the
///     layout when both ship together, e.g. bodhi's sidecar bundle); (c) the
///     first `bamboo` found on `$PATH`.
///
/// Takes its inputs as plain parameters instead of reading
/// `std::env`/`std::env::current_exe` itself — the caller
/// (`App::spawn_local_server`) snapshots the environment once, which keeps
/// this function pure and its tests parallel-safe (no process-global env
/// mutation to serialize against).
pub fn discover_bamboo_bin(
    env_override: Option<PathBuf>,
    exe_dir: Option<&Path>,
    path_var: Option<&str>,
) -> Option<PathBuf> {
    if let Some(path) = env_override {
        return Some(path);
    }
    let name = bamboo_bin_name();
    if let Some(dir) = exe_dir {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    if let Some(path_var) = path_var {
        for dir in std::env::split_paths(path_var) {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

// ── Main App ──

pub struct App {
    pub running: bool,
    pub tab: Tab,
    pub client: BambooClient,
    pub chat: ChatState,
    pub sessions: SessionsState,
    pub mcp: McpState,
    pub schedules: SchedulesState,
    pub skills: SkillsState,
    pub config: ConfigState,
    pub status_message: String,
    pub connected: bool,
    pub help_visible: bool,
    pub spinner_tick: usize,
    /// Startup-only "start a local server?" y/n offer; see [`ServeOffer`].
    /// Set at most once, by `run`'s initial health check, and cleared by
    /// `handle_serve_offer_key` before anything else can run — see the
    /// modal-precedence doc on `handle_key`.
    pub serve_offer: Option<ServeOffer>,
    /// The `bamboo serve` child this TUI spawned via auto-serve, if any.
    /// Held with `kill_on_drop(true)` so it dies when `App` (and hence this
    /// field) is dropped — the *graceful* death-link, covering a clean quit
    /// or panic-unwind. The *crash-safe* backstop (SIGKILL, force-quit — no
    /// Rust destructor runs at all) is the `--parent-pid <this pid>` flag
    /// passed to the child in `spawn_local_server`, which self-exits when it
    /// notices this process is gone — the same double death-link bodhi's
    /// sidecar uses (`bodhi/src-tauri/src/sidecar.rs`).
    pub spawned_server: Option<Child>,
    /// The agent's pending question (permission gate / clarification). When
    /// `Some`, a modal captures the answer and keystrokes route to it.
    pub pending_question: Option<ActiveQuestion>,
    /// The most recently *dismissed* (Esc'd) pending question, kept around
    /// because dismissing the modal does NOT tell the server to stop
    /// waiting — the run is still blocked on an answer. `Ctrl+Q` restores it
    /// without a round-trip; cleared once answered/resumed or on a fresh
    /// session (`Ctrl+N` / opening a different session).
    pub dismissed_question: Option<ActiveQuestion>,
    /// In-progress new-schedule form (Schedules tab). When `Some`, a modal
    /// captures the fields.
    pub schedule_form: Option<ScheduleForm>,
    /// In-progress raw-JSON config editor (Config tab). When `Some`, a modal
    /// textarea captures all keystrokes.
    pub config_editor: Option<ConfigEditor>,
    /// In-progress model picker (`Ctrl+O`, Chat tab). When `Some`, a modal
    /// captures navigation/apply keystrokes.
    pub model_picker: Option<ModelPicker>,
    /// In-progress contextual session picker (`Ctrl+P`, Chat tab). Unlike the
    /// Sessions management tab, closing this overlay leaves the transcript,
    /// composer draft/cursor, and scroll position untouched.
    pub session_picker: Option<SessionPicker>,
    /// Pending session-delete confirmation (Sessions tab, `d`): `(id, title)`
    /// of the session awaiting `y`/Enter confirm or `n`/Esc cancel. Kept as a
    /// modal (rather than deleting immediately) so a stray `d` can't destroy a
    /// session, and the actual DELETE runs off the event loop like every other
    /// mutation.
    pub pending_delete: Option<(String, String)>,
    /// Session id whose DELETE request is currently in flight. When it is the
    /// active Chat session, submission stays blocked until the result arrives
    /// so a concurrent chat POST cannot recreate the session being deleted.
    deleting_session_id: Option<String>,
    /// Capped scrollback of past status messages so errors/warnings aren't lost
    /// when the single status line is overwritten. Viewed with `Ctrl+L`.
    pub notifications: Vec<Notification>,
    /// Whether the notification-log overlay is open.
    pub notifications_visible: bool,
    /// Count of warn/error notifications since the log was last opened; shown as
    /// a badge in the status bar.
    pub unseen_alerts: usize,
    /// Monotonic epoch for answer submissions (see
    /// `AppEvent::AnswerSubmitted`): bumped by `submit_answer` when it spawns
    /// the POST (the in-flight task carries a copy) and by
    /// `supersede_pending_answer` whenever the question context changes
    /// underneath it, so a late response for a superseded question is
    /// discarded instead of applied to the wrong question/session.
    answer_epoch: u64,
    /// Tracked answer task so stopping, switching sessions, or superseding a
    /// question cancels the network operation itself.  Ignoring only the late
    /// result is insufficient: a task waiting for SSE readiness could still
    /// POST after the UI already reported the run stopped.
    answer_task: Option<tokio::task::JoinHandle<()>>,
    /// Free-text drafts survive dismissal, session switches, and SSE replay,
    /// keyed by the typed question identity rather than the active tab.
    question_drafts: HashMap<QuestionDraftKey, String>,
    /// Insertion/LRU order for the bounded draft cache. Drafts are useful
    /// across session switches, but must not accumulate for the lifetime of a
    /// long-running TUI process.
    question_draft_order: VecDeque<QuestionDraftKey>,
    /// Latest session whose async resume result is authoritative. An older
    /// request finishing later must not switch the operator back.
    opening_session_id: Option<String>,
    /// Shared monotonic identity for picker fetches/mutations. Closing and
    /// reopening a picker invalidates every result from the previous overlay.
    picker_epoch: u64,
    model_picker_task: Option<tokio::task::JoinHandle<()>>,
    session_picker_task: Option<tokio::task::JoinHandle<()>>,
    recent_models: VecDeque<(String, String)>,
    /// Sender into the main event loop, used to post results of background API
    /// calls (so those calls never block the UI thread). Set in [`run`].
    event_tx: Option<mpsc::UnboundedSender<AppEvent>>,
    sse_tx: Option<mpsc::UnboundedSender<SessionSseEvent>>,
    sse_rx: Option<mpsc::UnboundedReceiver<SessionSseEvent>>,
    sse_task: Option<tokio::task::JoinHandle<()>>,
    /// Monotonic identity for an attached SSE task. Control/data events from a
    /// detached generation are ignored even if they were already queued when
    /// a replacement stream was installed.
    sse_epoch: u64,
    /// Monotonic identity for pending-question reconciliation requests. One
    /// SSE generation can reconnect more than once; a slower earlier GET must
    /// never overwrite a newer authoritative snapshot for the same session
    /// and answer epoch.
    pending_reconcile_epoch: u64,
    /// Handshake state for the current SSE generation. Pending answers wait
    /// until this is true before POSTing because the server may resume the run
    /// before the respond HTTP request returns.
    sse_ready: Option<watch::Receiver<bool>>,
    /// The agent may still be running but its event transport is unavailable.
    /// Kept separate from `chat.streaming` so input stays blocked and partial
    /// output remains intact while the operator can still request Stop.
    pub stream_disconnected: bool,
    /// A new execution generation was observed after the currently-submitting
    /// clarification answer. Its Complete is a real terminal event even if the
    /// respond HTTP result has not yet arrived to clear the local modal.
    pending_answer_run_started: bool,
}

/// Move a list selection by `delta` (positive = down, negative = up),
/// clamped to `[0, len-1]` (or `0` when the list is empty). Shared by every
/// list tab's mouse-wheel handling in `App::handle_mouse`.
fn scroll_selection(selected: usize, len: usize, delta: i32) -> usize {
    if len == 0 {
        return 0;
    }
    let next = selected as i64 + delta as i64;
    next.clamp(0, (len - 1) as i64) as usize
}

fn question_option_at(question: &ActiveQuestion, column: u16, row: u16) -> Option<usize> {
    if question.inspecting || question.custom.is_some() || question.number_entry.is_some() {
        return None;
    }
    question
        .option_hitboxes
        .borrow()
        .iter()
        .find(|hitbox| hitbox.contains(column, row))
        .map(|hitbox| hitbox.index)
}

/// Copy text through the terminal-standard OSC 52 clipboard sequence. The
/// payload is base64 encoded, so arbitrary Unicode and line breaks remain
/// exact and never terminate the control sequence early.
fn copy_via_osc52(value: &str) -> std::io::Result<()> {
    let encoded = BASE64_STANDARD.encode(value.as_bytes());
    let mut stdout = std::io::stdout().lock();
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()
}

impl App {
    pub fn new(client: BambooClient) -> Self {
        Self {
            running: true,
            tab: Tab::Chat,
            client,
            chat: ChatState::new(),
            sessions: SessionsState::new(),
            mcp: McpState::new(),
            schedules: SchedulesState::new(),
            skills: SkillsState::new(),
            config: ConfigState::new(),
            status_message: String::new(),
            connected: false,
            help_visible: false,
            spinner_tick: 0,
            serve_offer: None,
            spawned_server: None,
            pending_question: None,
            dismissed_question: None,
            schedule_form: None,
            config_editor: None,
            model_picker: None,
            session_picker: None,
            pending_delete: None,
            deleting_session_id: None,
            notifications: Vec::new(),
            notifications_visible: false,
            unseen_alerts: 0,
            answer_epoch: 0,
            answer_task: None,
            question_drafts: HashMap::new(),
            question_draft_order: VecDeque::new(),
            opening_session_id: None,
            picker_epoch: 0,
            model_picker_task: None,
            session_picker_task: None,
            recent_models: VecDeque::new(),
            event_tx: None,
            sse_tx: None,
            sse_rx: None,
            sse_task: None,
            sse_epoch: 0,
            pending_reconcile_epoch: 0,
            sse_ready: None,
            stream_disconnected: false,
            pending_answer_run_started: false,
        }
    }

    pub async fn run(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
        auto_serve_mode: AutoServeMode,
    ) -> Result<()> {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<AppEvent>();
        // Keep a sender so background API tasks can post their results back.
        // Set BEFORE the initial health check below: for `AutoServeMode::Auto`
        // that check may call `spawn_local_server`, which needs this sender to
        // post its health-poll waiter's result back as `LocalServerReady`.
        self.event_tx = Some(event_tx.clone());

        self.connected = self.client.health().await.unwrap_or(false);
        if self.connected {
            self.status_message = "Connected".to_string();
        } else {
            let url = self.client.base_url.clone();
            match (is_loopback_url(&url), auto_serve_mode) {
                (true, AutoServeMode::Auto) => self.spawn_local_server(),
                (true, AutoServeMode::Prompt) => {
                    self.status_message = format!(
                        "Bamboo server is not reachable at {url}. Start a local server? (y/n)"
                    );
                    self.serve_offer = Some(ServeOffer { url });
                }
                // Loopback but auto-serve explicitly disabled, or a remote URL
                // (never auto-started regardless of flags): keep the previous
                // "just warn" behavior, improved to name the URL.
                (true, AutoServeMode::Off) | (false, _) => {
                    self.notify(
                        NoticeLevel::Warn,
                        format!("Cannot connect to server at {url}"),
                    );
                }
            }
        }

        // Kick off the initial tab's data load without blocking startup.
        self.load_tab_data();

        // `--session-id` on the command line: resume it the same way `Enter`
        // on the Sessions tab does (history replay + live reattach + pending
        // question recovery), not a bespoke startup-only path.
        if let Some(session_id) = self.chat.session_id.clone() {
            self.resume_session(session_id);
        }

        // Spawn crossterm event reader.
        let tx = event_tx.clone();
        tokio::spawn(async move {
            let mut reader = EventStream::new();
            while let Some(event) = reader.next().await {
                match event {
                    Ok(Event::Key(key)) if tx.send(AppEvent::Key(key)).is_err() => {
                        break;
                    }
                    Ok(Event::Mouse(mouse)) if tx.send(AppEvent::Mouse(mouse)).is_err() => {
                        break;
                    }
                    Ok(Event::Resize(_, _)) if tx.send(AppEvent::Resize).is_err() => {
                        break;
                    }
                    _ => {}
                }
            }
        });

        // Redraw ticker: the loop below only otherwise iterates (and redraws)
        // when a key/mouse event or an SSE event arrives, so a long tool call
        // with no token traffic would freeze the braille spinner and make the
        // UI look hung. Created ONCE here (not inside the loop) so `tick()`
        // correctly accounts for elapsed time across iterations instead of
        // firing immediately every time.
        let mut redraw_interval = tokio::time::interval(std::time::Duration::from_millis(120));

        // Main event loop.
        while self.running {
            self.poll_sse();
            self.spinner_tick = self.spinner_tick.wrapping_add(1);
            terminal.draw(|f| ui::render(f, self))?;

            tokio::select! {
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_event(event).await?;
                    }
                }
                sse_event = async {
                    if let Some(rx) = &mut self.sse_rx {
                        rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    if let Some(event) = sse_event {
                        if let Err(e) = self.handle_session_sse_event(event) {
                            self.notify(NoticeLevel::Error, format!("SSE error: {e}"));
                        }
                    }
                }
                _ = redraw_interval.tick() => {
                    // No new state — just a steady redraw so the spinner animates.
                }
            }
        }

        Ok(())
    }

    fn poll_sse(&mut self) {
        let events: Vec<SessionSseEvent> = if let Some(rx) = &mut self.sse_rx {
            std::iter::from_fn(|| rx.try_recv().ok()).collect()
        } else {
            Vec::new()
        };
        for event in events {
            if let Err(e) = self.handle_session_sse_event(event) {
                self.notify(NoticeLevel::Error, format!("SSE error: {e}"));
            }
        }
    }

    async fn handle_event(&mut self, event: AppEvent) -> Result<()> {
        if self.help_visible && !self.any_modal_open() {
            if let AppEvent::Key(_) = &event {
                self.help_visible = false;
                return Ok(());
            }
        }

        // The notification-log overlay is dismissed by any key.
        if self.notifications_visible && !self.any_modal_open() {
            if let AppEvent::Key(_) = &event {
                self.notifications_visible = false;
                return Ok(());
            }
        }

        match event {
            AppEvent::Key(key) => self.handle_key(key).await?,
            AppEvent::Mouse(mouse) => self.handle_mouse(mouse),
            AppEvent::SessionsLoaded(r) => {
                self.sessions.loading = false;
                match r {
                    Ok(envelope) => {
                        self.sessions.selected = self
                            .sessions
                            .selected
                            .min(envelope.sessions.len().saturating_sub(1));
                        self.sessions.total = envelope.total;
                        self.sessions.offset = envelope.offset;
                        self.sessions.page_limit = envelope.limit;
                        self.sessions.next_offset = envelope.next_offset;
                        self.sessions.sessions = envelope.sessions;
                        self.sessions.error = None;
                    }
                    Err(e) => self.sessions.error = Some(e),
                }
            }
            AppEvent::McpServersLoaded(r) => {
                self.mcp.loading = false;
                match r {
                    Ok(s) => {
                        self.mcp.selected = self.mcp.selected.min(s.len().saturating_sub(1));
                        self.mcp.servers = s;
                        self.mcp.error = None;
                    }
                    Err(e) => self.mcp.error = Some(e),
                }
            }
            AppEvent::McpToolsLoaded(r) => {
                self.mcp.loading = false;
                match r {
                    Ok(t) => {
                        self.mcp.tools = t;
                        self.mcp.error = None;
                    }
                    Err(e) => self.mcp.error = Some(e),
                }
            }
            AppEvent::SchedulesLoaded(r) => {
                self.schedules.loading = false;
                match r {
                    Ok(s) => {
                        self.schedules.selected =
                            self.schedules.selected.min(s.len().saturating_sub(1));
                        self.schedules.schedules = s;
                        self.schedules.error = None;
                    }
                    Err(e) => self.schedules.error = Some(e),
                }
            }
            AppEvent::SkillsLoaded(r) => {
                self.skills.loading = false;
                match r {
                    Ok(s) => {
                        self.skills.skills = s;
                        self.skills.error = None;
                    }
                    Err(e) => self.skills.error = Some(e),
                }
            }
            AppEvent::ConfigLoaded(r) => {
                self.config.loading = false;
                match r {
                    Ok(c) => {
                        self.config.config = Some(c);
                        self.config.error = None;
                    }
                    Err(e) => self.config.error = Some(e),
                }
            }
            AppEvent::ActionDone {
                outcome,
                reload_tab,
                session_picker_epoch,
            } => {
                let succeeded = outcome.is_ok();
                match outcome {
                    Ok(msg) => self.notify(NoticeLevel::Info, msg),
                    Err(msg) => self.notify(NoticeLevel::Error, msg),
                }
                if reload_tab {
                    let reload_origin_picker = succeeded
                        && session_picker_epoch.is_some_and(|epoch| {
                            self.session_picker.as_ref().is_some_and(|picker| {
                                picker.epoch == epoch
                                    && matches!(picker.mode, SessionPickerMode::Browse)
                            })
                        });
                    if reload_origin_picker {
                        self.reload_session_picker();
                    }
                    self.load_tab_data();
                }
            }
            AppEvent::SessionDeleted {
                session_id,
                result,
                session_picker_epoch,
            } => {
                if self.deleting_session_id.as_deref() == Some(session_id.as_str()) {
                    self.deleting_session_id = None;
                }
                let succeeded = result.is_ok();
                let deleted_active =
                    succeeded && self.chat.session_id.as_deref() == Some(session_id.as_str());
                let deleted_opening =
                    succeeded && self.opening_session_id.as_deref() == Some(session_id.as_str());
                if deleted_active {
                    // Detach before another input event can be handled. This
                    // preserves the deliberate model/Project/composer draft,
                    // while removing every server-backed trace of the deleted
                    // session from the Chat view. Preserve an unrelated newer
                    // resume, but never let a queued resume of this deleted id
                    // become authoritative again.
                    let unrelated_opening = self
                        .opening_session_id
                        .take()
                        .filter(|opening| opening != &session_id);
                    self.new_session();
                    self.opening_session_id = unrelated_opening;
                } else if deleted_opening {
                    // The resume task itself is best-effort and may already
                    // have queued a result. Clearing its authoritative id
                    // makes that stale SessionOpened harmless.
                    self.opening_session_id = None;
                }
                match result {
                    Ok(()) => self.notify(
                        NoticeLevel::Info,
                        if deleted_active {
                            "Session deleted — started a new session"
                        } else {
                            "Session deleted"
                        },
                    ),
                    Err(error) => {
                        self.notify(NoticeLevel::Error, format!("Delete failed: {error}"))
                    }
                }
                let reload_origin_picker = succeeded
                    && session_picker_epoch.is_some_and(|epoch| {
                        self.session_picker.as_ref().is_some_and(|picker| {
                            picker.epoch == epoch
                                && matches!(picker.mode, SessionPickerMode::Browse)
                        })
                    });
                if reload_origin_picker {
                    self.reload_session_picker();
                }
                self.load_tab_data();
            }
            AppEvent::ChatStarted(r) => match r {
                Ok(session_id) => {
                    self.chat.session_id = Some(session_id.clone());
                    self.status_message = "Streaming...".to_string();
                    self.start_stream_and_execute(session_id);
                }
                Err(e) => {
                    self.chat.streaming = false;
                    self.notify(NoticeLevel::Error, format!("Error: {e}"));
                }
            },
            AppEvent::ExecuteFailed(msg) => {
                // The POST that starts the run never succeeded, so no SSE
                // terminal event is ever coming — finalize here or
                // `chat.streaming` spins forever.
                self.notify(NoticeLevel::Error, format!("Failed to start run: {msg}"));
                self.finalize_streaming();
            }
            AppEvent::StopFinished(r) => {
                // Finalize regardless of outcome: even if the stop request
                // failed (server down/unreachable), the operator must regain
                // control of the input instead of being stuck waiting for a
                // terminal SSE event that a dead server will never send.
                // `finalize_streaming` resets `status_message` to "Ready"
                // internally, so the outcome-specific message is set AFTER it
                // (same ordering the old synchronous `stop_streaming` used to
                // get "Stopped" to stick instead of being overwritten).
                self.finalize_streaming();
                match r {
                    Ok(()) => self.status_message = "Stopped".to_string(),
                    Err(e) => self.notify(NoticeLevel::Error, format!("Stop failed: {e}")),
                }
            }
            AppEvent::SkillDetailLoaded(r) => match r {
                Ok(detail) => {
                    self.skills.detail = Some(detail);
                    self.skills.error = None;
                }
                Err(e) => self.skills.error = Some(e),
            },
            AppEvent::SessionOpened { session_id, result } => {
                if self.opening_session_id.as_deref() != Some(session_id.as_str()) {
                    return Ok(());
                }
                self.opening_session_id = None;
                match result {
                    Ok(opened) => {
                        // Contextual pickers are bound to the chat session
                        // visible beneath them. A concurrently completing
                        // resume invalidates that context and any in-flight
                        // model PATCH result before installing the new session.
                        if self.session_picker.is_some() {
                            self.pending_delete = None;
                        }
                        self.close_session_picker();
                        self.close_model_picker();
                        // Reset every per-run scratch field `finalize_streaming`
                        // would otherwise leave behind from whatever was open
                        // before — a resumed session must not inherit stale
                        // in-flight-turn state from a prior chat.
                        self.detach_stream();
                        self.chat.session_id = Some(session_id.clone());
                        self.chat.model = opened.model;
                        self.chat.provider = opened.provider;
                        self.chat.project_id = opened.project_id;
                        self.chat.current_response.clear();
                        self.chat.current_tool_calls.clear();
                        self.chat.current_reasoning.clear();
                        self.chat.sub_agents.clear();
                        self.chat.token_usage = None;
                        self.chat.scroll_offset = 0;
                        self.chat.streaming = false;
                        self.stream_disconnected = false;
                        self.pending_answer_run_started = false;
                        self.stash_question_drafts();
                        self.supersede_pending_answer();
                        self.pending_question = None;
                        self.dismissed_question = None;

                        let shown = opened.messages.len();
                        self.chat.messages = opened.messages;
                        self.chat.auto_scroll = true;
                        self.tab = Tab::Chat;
                        self.status_message = "Session resumed".to_string();

                        if opened.truncated {
                            self.notify(
                                NoticeLevel::Info,
                                format!(
                                    "Showing last {shown} of {} messages",
                                    opened.total_message_count
                                ),
                            );
                        }

                        // A persisted question is an idle pause, but answering
                        // it resumes execution inside the respond handler
                        // before that HTTP response returns. Subscribe now so
                        // even an immediate resumed Token/Complete is observed.
                        if opened.is_running || opened.pending.is_some() {
                            self.attach_stream(session_id.clone());
                            self.chat.streaming = true;
                            self.status_message = if opened.is_running {
                                "Reattached — streaming".to_string()
                            } else {
                                "Reattached — waiting for answer".to_string()
                            };
                        }

                        if let Some(pending) = &opened.pending {
                            self.status_message =
                                format!("Question: {} (answer in the dialog)", pending.question);
                            let question = self.question_from_pending(session_id, pending);
                            self.pending_question = Some(question);
                        }
                    }
                    Err(e) => {
                        self.notify(NoticeLevel::Error, format!("Failed to open session: {e}"));
                    }
                }
            }
            AppEvent::PendingQuestionChecked {
                session_id,
                epoch,
                result,
            } => {
                if epoch != self.answer_epoch
                    || self.chat.session_id.as_deref() != Some(session_id.as_str())
                {
                    return Ok(());
                }
                match result {
                    Ok(pending) if pending.has_pending_question => {
                        self.stash_question_drafts();
                        self.supersede_pending_answer();
                        let question = self.question_from_pending(session_id.clone(), &pending);
                        self.pending_question = Some(question);
                        // Ctrl+Q may recover a server-side pause after the old
                        // event stream was detached. Treat it as an active run
                        // again so Ctrl+C routes to STOP (instead of quitting
                        // the TUI), and subscribe before any answer can resume.
                        self.attach_stream(session_id);
                        self.chat.streaming = true;
                        self.status_message = "Question reopened".to_string();
                    }
                    Ok(_) => {
                        self.notify(NoticeLevel::Info, "No pending question on the server");
                    }
                    Err(e) => {
                        self.notify(
                            NoticeLevel::Error,
                            format!("Failed to check pending question: {e}"),
                        );
                    }
                }
            }
            AppEvent::PendingQuestionReconciled {
                session_id,
                epoch,
                reconcile_epoch,
                result,
            } => {
                if epoch != self.answer_epoch
                    || reconcile_epoch != self.pending_reconcile_epoch
                    || self.chat.session_id.as_deref() != Some(session_id.as_str())
                    || self
                        .pending_question
                        .as_ref()
                        .is_some_and(|question| question.submitting)
                {
                    return Ok(());
                }
                match result {
                    Ok(pending) if pending.has_pending_question => {
                        let incoming = self.question_from_pending(session_id, &pending);
                        if let Some(existing) = self.pending_question.as_mut() {
                            if existing.identity() == incoming.identity()
                                || existing.can_hydrate_identity_from(&incoming)
                            {
                                existing.refresh_contract(incoming);
                                return Ok(());
                            }
                        } else if let Some(dismissed) = self.dismissed_question.as_mut() {
                            if dismissed.identity() == incoming.identity()
                                || dismissed.can_hydrate_identity_from(&incoming)
                            {
                                dismissed.refresh_contract(incoming);
                                return Ok(());
                            }
                        }

                        self.stash_question_drafts();
                        self.supersede_pending_answer();
                        self.dismissed_question = None;
                        self.status_message = format!(
                            "Question recovered after stream sync: {}",
                            incoming.question
                        );
                        self.pending_question = Some(incoming);
                    }
                    Ok(_) => {
                        if let Some(question) = self.pending_question.as_mut() {
                            if question.identity_syncing {
                                question.identity_syncing = false;
                                question.error = Some(
                                    "The server did not expose an exact response identity; this question cannot be answered from the TUI yet"
                                        .to_string(),
                                );
                                self.status_message =
                                    "Question identity unavailable — answer not sent".to_string();
                                return Ok(());
                            }
                        }
                        if self.pending_question.is_some() || self.dismissed_question.is_some() {
                            self.stash_question_drafts();
                            self.supersede_pending_answer();
                            self.pending_question = None;
                            self.dismissed_question = None;
                            self.status_message =
                                "Pending question cleared after stream sync".to_string();
                        }
                    }
                    Err(error) => self.notify(
                        NoticeLevel::Error,
                        format!("Failed to reconcile question after SSE connect: {error}"),
                    ),
                }
            }
            AppEvent::PendingQuestionRefreshed {
                session_id,
                epoch,
                identity,
                result,
            } => {
                if epoch != self.answer_epoch
                    || self.chat.session_id.as_deref() != Some(session_id.as_str())
                    || self
                        .pending_question
                        .as_ref()
                        .is_none_or(|question| question.identity() != identity)
                {
                    return Ok(());
                }
                match result {
                    Ok(pending) if pending.has_pending_question => {
                        self.stash_question_drafts();
                        self.supersede_pending_answer();
                        let question = self.question_from_pending(session_id, &pending);
                        self.pending_question = Some(question);
                        self.status_message =
                            "Question refreshed after rejected answer".to_string();
                    }
                    Ok(_) => {
                        self.remove_question_drafts_for_identity(&identity);
                        self.supersede_pending_answer();
                        self.pending_question = None;
                        self.dismissed_question = None;
                        self.status_message = "Question was already answered".to_string();
                        self.notify(
                            NoticeLevel::Info,
                            "The pending question no longer exists on the server",
                        );
                    }
                    Err(error) => {
                        if let Some(question) = self.pending_question.as_mut() {
                            question.submitting = false;
                            question.error =
                                Some(format!("Could not refresh after rejection: {error}"));
                        }
                        self.notify(
                            NoticeLevel::Error,
                            format!("Failed to refresh pending question: {error}"),
                        );
                    }
                }
            }
            AppEvent::AnswerSubmitted {
                epoch,
                identity,
                answer,
                result,
            } => {
                // A late response for a question that has since been
                // superseded (new question arrived, session switched, run
                // finalized, modal reopened — every one of those bumps
                // `answer_epoch` via `supersede_pending_answer`) must be
                // discarded outright: applying it would clear or resume state
                // that belongs to a different question than the one this
                // answer was for.
                if epoch != self.answer_epoch
                    || self
                        .pending_question
                        .as_ref()
                        .is_none_or(|question| question.identity() != identity)
                {
                    return Ok(());
                }
                self.answer_task.take();
                match result {
                    Ok(status) => {
                        self.remove_question_drafts_for_identity(&identity);
                        self.pending_question = None;
                        if self
                            .dismissed_question
                            .as_ref()
                            .is_some_and(|question| question.identity() == identity)
                        {
                            self.dismissed_question = None;
                        }
                        // Only keep the spinner on if a run is actually
                        // running: the server returns 200 even when it did
                        // NOT resume (e.g. the session already `completed`),
                        // so a blind `streaming = true` would spin forever.
                        // Normally the original stream stays attached across
                        // the question. If transport retries gave up while it
                        // was open, a successful POST proves the server is
                        // reachable again, so reattach before waiting for the
                        // resumed run's events.
                        if matches!(status.as_str(), "started" | "already_running") {
                            if !self.stream_is_ready() {
                                self.attach_stream(identity.session_id.clone());
                            }
                            self.status_message = format!("Answered: {answer} — resuming");
                            self.chat.streaming = true;
                        } else {
                            self.finalize_streaming();
                            self.status_message = format!("Answered: {answer} ({status})");
                        }
                    }
                    Err(error) if error.should_refresh_question() => {
                        let message = error.to_string();
                        self.notify(
                            NoticeLevel::Warn,
                            format!("Answer rejected; refreshing question state: {message}"),
                        );
                        self.refresh_question_after_rejection(identity, message);
                    }
                    Err(error) => {
                        let message = error.to_string();
                        // Transport/server failures do not prove the question
                        // changed. Keep the modal open with input enabled so
                        // the operator can retry the exact submission.
                        if let Some(q) = self.pending_question.as_mut() {
                            q.submitting = false;
                            q.error = Some(message.clone());
                        }
                        self.notify(NoticeLevel::Error, format!("Answer rejected: {message}"));
                    }
                }
            }
            AppEvent::CatalogLoaded { epoch, result } => {
                let selected_key = self
                    .model_picker
                    .as_ref()
                    .filter(|picker| picker.epoch == epoch)
                    .and_then(ModelPicker::selected_model)
                    .map(model_key);
                let Some(picker) = self
                    .model_picker
                    .as_mut()
                    .filter(|picker| picker.epoch == epoch)
                else {
                    return Ok(());
                };
                self.model_picker_task = None;
                picker.loading = false;
                match result {
                    Ok(mut catalog) => {
                        catalog.models.retain(|model| {
                            !model.reference.provider.trim().is_empty()
                                && !model.reference.model.trim().is_empty()
                        });
                        let current = current_model_key(
                            &catalog.models,
                            self.chat.provider.as_deref(),
                            &self.chat.model,
                        );
                        sort_catalog_models(
                            &mut catalog.models,
                            current.as_ref(),
                            &self.recent_models,
                        );
                        picker.models = catalog.models;
                        picker.error = picker.models.is_empty().then(|| {
                            "No models in provider catalog — press Ctrl+R to retry".to_string()
                        });
                        picker.refresh_filter(selected_key);
                    }
                    Err(error) => {
                        picker.error = Some(format!(
                            "Failed to load provider catalog: {error} — press Ctrl+R to retry"
                        ));
                    }
                }
            }
            AppEvent::ModelPatched {
                epoch,
                session_id,
                model,
                result,
            } => {
                let Some(picker) = self
                    .model_picker
                    .as_mut()
                    .filter(|picker| picker.epoch == epoch)
                else {
                    return Ok(());
                };
                self.model_picker_task = None;
                picker.applying = false;
                if self.chat.session_id.as_deref() != Some(session_id.as_str()) {
                    picker.error = Some(
                        "Active session changed while applying the model — reopen the picker"
                            .to_string(),
                    );
                    return Ok(());
                }
                match result {
                    Ok(()) => self.commit_model_selection(model),
                    Err(error) => {
                        picker.error = Some(format!(
                            "Failed to update session model: {error} — press Enter to retry or Esc to cancel"
                        ));
                    }
                }
            }
            AppEvent::SessionPickerPageLoaded {
                epoch,
                offset,
                result,
            } => {
                let Some(picker) = self
                    .session_picker
                    .as_mut()
                    .filter(|picker| picker.epoch == epoch)
                else {
                    return Ok(());
                };
                self.session_picker_task = None;
                picker.loading = false;
                let selected_before = picker.selected_session().map(|session| session.id.clone());
                let loaded_page = result.is_ok();
                match result {
                    Ok(envelope) => {
                        if offset == 0 {
                            picker.sessions.clear();
                        }
                        for session in envelope.sessions {
                            if picker.sessions.len() >= MAX_SESSION_PICKER_SESSIONS {
                                break;
                            }
                            if let Some(existing) = picker
                                .sessions
                                .iter_mut()
                                .find(|existing| existing.id == session.id)
                            {
                                *existing = session;
                            } else {
                                picker.sessions.push(session);
                            }
                        }
                        picker.total = envelope.total;
                        picker.page_limit = envelope.limit;
                        picker.next_offset = (picker.sessions.len() < MAX_SESSION_PICKER_SESSIONS)
                            .then_some(envelope.next_offset)
                            .flatten()
                            .filter(|next| *next > offset);
                        picker.error = None;
                        let preserve_id = if picker.selection_touched {
                            selected_before
                        } else {
                            self.chat
                                .session_id
                                .clone()
                                .filter(|active_id| {
                                    picker
                                        .sessions
                                        .iter()
                                        .any(|session| &session.id == active_id)
                                })
                                .or(selected_before)
                        };
                        picker.refresh_filter(preserve_id);
                    }
                    Err(error) => {
                        picker.error = Some(format!(
                            "Failed to load sessions: {error} — press Ctrl+R to retry"
                        ));
                    }
                }
                let should_continue = self.session_picker.as_ref().is_some_and(|picker| {
                    loaded_page
                        && !picker.query.is_empty()
                        && picker.next_offset.is_some()
                        && picker.sessions.len() < MAX_SESSION_PICKER_SESSIONS
                });
                if should_continue {
                    self.load_next_session_picker_page();
                }
            }
            AppEvent::SessionPickerVersionLoaded {
                epoch,
                session_id,
                intent,
                result,
            } => {
                let Some(picker) = self
                    .session_picker
                    .as_mut()
                    .filter(|picker| picker.epoch == epoch)
                else {
                    return Ok(());
                };
                if !picker.mode.matches(&session_id, &intent) {
                    return Ok(());
                }
                self.session_picker_task = None;
                let mut patch = None;
                match result {
                    Ok(versioned) => {
                        let fresh_title = versioned.summary.title.clone();
                        let fresh_version = versioned.metadata_version;
                        if let Some(existing) = picker
                            .sessions
                            .iter_mut()
                            .find(|session| session.id == session_id)
                        {
                            *existing = versioned.summary;
                        }
                        match &mut picker.mode {
                            SessionPickerMode::Rename {
                                draft,
                                base_title,
                                draft_dirty,
                                metadata_version,
                                loading_version,
                                error,
                                ..
                            } => {
                                *loading_version = false;
                                if *draft_dirty && *base_title != fresh_title {
                                    // The user edited a title derived from a
                                    // stale list row. Do not bind that draft to
                                    // a newer ETag until the conflict has been
                                    // made visible and explicitly retried.
                                    *metadata_version = None;
                                    *base_title = fresh_title;
                                    *error = Some(
                                        "Title changed on the server; draft preserved — press Ctrl+R to confirm against the latest title"
                                            .to_string(),
                                    );
                                } else {
                                    if !*draft_dirty {
                                        *draft = fresh_title.clone();
                                    }
                                    *base_title = fresh_title;
                                    *metadata_version = Some(fresh_version);
                                    *error = None;
                                }
                            }
                            SessionPickerMode::Pinning {
                                target,
                                loading_version,
                                submitting,
                                error,
                                ..
                            } => {
                                *loading_version = false;
                                *submitting = true;
                                *error = None;
                                patch = Some((fresh_version, *target));
                            }
                            SessionPickerMode::Browse => {}
                        }
                    }
                    Err(error_message) => match &mut picker.mode {
                        SessionPickerMode::Rename {
                            loading_version,
                            error,
                            ..
                        }
                        | SessionPickerMode::Pinning {
                            loading_version,
                            error,
                            ..
                        } => {
                            *loading_version = false;
                            *error = Some(format!(
                                "Failed to prepare update: {error_message} — press Ctrl+R to retry"
                            ));
                        }
                        SessionPickerMode::Browse => {}
                    },
                }
                if let Some((version, target)) = patch {
                    self.spawn_session_picker_patch(
                        epoch,
                        session_id,
                        version,
                        SessionPickerIntent::Pin(target),
                        PatchSessionMetadataRequest {
                            title: None,
                            pinned: Some(target),
                        },
                    );
                }
            }
            AppEvent::SessionPickerPatched {
                epoch,
                session_id,
                intent,
                result,
            } => {
                let Some(picker) = self
                    .session_picker
                    .as_mut()
                    .filter(|picker| picker.epoch == epoch)
                else {
                    return Ok(());
                };
                if !picker.mode.matches(&session_id, &intent) {
                    return Ok(());
                }
                self.session_picker_task = None;
                match result {
                    Ok(versioned) => {
                        if let Some(existing) = picker
                            .sessions
                            .iter_mut()
                            .find(|session| session.id == session_id)
                        {
                            *existing = versioned.summary;
                        }
                        picker.mode = SessionPickerMode::Browse;
                        picker.refresh_filter(Some(session_id));
                        self.status_message = match intent {
                            SessionPickerIntent::Rename => "Session renamed".to_string(),
                            SessionPickerIntent::Pin(true) => "Session pinned".to_string(),
                            SessionPickerIntent::Pin(false) => "Session unpinned".to_string(),
                        };
                    }
                    Err(failure) => {
                        let prefix = if failure.conflict {
                            "Version conflict; refetch before retry"
                        } else {
                            "Session update failed"
                        };
                        let current = failure
                            .current_version
                            .map(|version| format!(" (server version {version})"))
                            .unwrap_or_default();
                        let message =
                            format!("{prefix}{current}: {failure} — press Ctrl+R to retry");
                        match &mut picker.mode {
                            SessionPickerMode::Rename {
                                metadata_version,
                                submitting,
                                error,
                                ..
                            } => {
                                *metadata_version = None;
                                *submitting = false;
                                *error = Some(message);
                            }
                            SessionPickerMode::Pinning {
                                submitting, error, ..
                            } => {
                                *submitting = false;
                                *error = Some(message);
                            }
                            SessionPickerMode::Browse => {}
                        }
                    }
                }
            }
            AppEvent::LocalServerReady(Ok(pid)) => {
                self.connected = true;
                self.notify(
                    NoticeLevel::Info,
                    format!("Local server started (pid {pid})"),
                );
                self.load_tab_data();
                // The `--session-id` startup resume (in `run`) ran while
                // disconnected and so left `chat.session_id` set but
                // `chat.messages` empty — retry it now that the server is up.
                if self.chat.messages.is_empty() {
                    if let Some(session_id) = self.chat.session_id.clone() {
                        self.resume_session(session_id);
                    }
                }
            }
            AppEvent::LocalServerReady(Err(e)) => {
                self.notify(
                    NoticeLevel::Error,
                    format!("Local server failed to start: {e}"),
                );
                if let Some(mut child) = self.spawned_server.take() {
                    let _ = child.start_kill();
                }
                self.connected = false;
            }
            _ => {}
        }
        Ok(())
    }

    /// Mouse wheel: on the two scrollable-text tabs (Chat/Config) it scrolls
    /// the view; everywhere else (the four list tabs) there's nothing to
    /// scroll independently of the selection, so the wheel moves the
    /// selection instead (3 rows per notch, clamped) rather than being a
    /// dead input.
    fn handle_mouse(&mut self, mouse: crossterm::event::MouseEvent) {
        use crossterm::event::{MouseButton, MouseEventKind};
        if self.pending_question.is_some() {
            let mut submit = None;
            {
                let question = self.pending_question.as_mut().unwrap();
                if question.submitting {
                    return;
                }
                match mouse.kind {
                    MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                        let delta: i32 = if matches!(mouse.kind, MouseEventKind::ScrollUp) {
                            -3
                        } else {
                            3
                        };
                        if question.inspecting {
                            let max_scroll = question.inspect_max_scroll.get();
                            let current = question.inspect_scroll.min(max_scroll);
                            if delta < 0 {
                                question.inspect_scroll =
                                    current.saturating_sub(delta.unsigned_abs() as u16);
                            } else {
                                question.inspect_scroll =
                                    current.saturating_add(delta as u16).min(max_scroll);
                            }
                        } else if question.custom.is_none() && !question.options.is_empty() {
                            question.selected =
                                scroll_selection(question.selected, question.options.len(), delta);
                            question.error = None;
                        }
                    }
                    MouseEventKind::Down(MouseButton::Left) => {
                        question.mouse_pressed_option =
                            question_option_at(question, mouse.column, mouse.row);
                    }
                    MouseEventKind::Up(MouseButton::Left) => {
                        let pressed = question.mouse_pressed_option.take();
                        if let Some(index) = question_option_at(question, mouse.column, mouse.row)
                            .filter(|index| Some(*index) == pressed)
                        {
                            // Commit the selection only on release. Updating it
                            // on mouse-down recenters a long option window on
                            // the next redraw and moves the hitboxes before the
                            // matching mouse-up arrives.
                            question.selected = index;
                            question.error = None;
                            submit = question.options.get(index).cloned();
                        }
                    }
                    _ => {
                        question.mouse_pressed_option = None;
                    }
                }
            }
            if let Some(answer) = submit {
                self.submit_answer(answer);
            }
            return;
        }

        // Non-pointer modal owners still consume mouse input. In particular,
        // a delete confirmation must not let the wheel move the Session row
        // hidden behind it; cancel then returns to the exact prior selection.
        if self.serve_offer.is_some()
            || self.pending_delete.is_some()
            || self.schedule_form.is_some()
            || self.config_editor.is_some()
        {
            return;
        }

        if let Some(picker) = self.session_picker.as_mut() {
            if matches!(picker.mode, SessionPickerMode::Browse) {
                let delta = match mouse.kind {
                    MouseEventKind::ScrollUp => -3,
                    MouseEventKind::ScrollDown => 3,
                    _ => return,
                };
                picker.selected = scroll_selection(picker.selected, picker.visible.len(), delta);
                picker.selection_touched = true;
            }
            return;
        }

        if let Some(picker) = self.model_picker.as_mut() {
            let delta = match mouse.kind {
                MouseEventKind::ScrollUp => -3,
                MouseEventKind::ScrollDown => 3,
                _ => return,
            };
            if !picker.loading && !picker.applying {
                picker.selected = scroll_selection(picker.selected, picker.visible.len(), delta);
            }
            return;
        }

        let delta: i32 = match mouse.kind {
            MouseEventKind::ScrollUp => -3,
            MouseEventKind::ScrollDown => 3,
            _ => return,
        };
        match self.tab {
            Tab::Chat => {
                if delta < 0 {
                    self.chat_scroll_up(delta.unsigned_abs() as u16);
                } else {
                    self.chat_scroll_down(delta as u16);
                }
            }
            Tab::Config => {
                if delta < 0 {
                    self.config_scroll_up(delta.unsigned_abs() as u16);
                } else {
                    self.config_scroll_down(delta as u16);
                }
            }
            Tab::Sessions => {
                self.sessions.selected =
                    scroll_selection(self.sessions.selected, self.sessions.sessions.len(), delta);
            }
            Tab::Mcp => {
                self.mcp.selected =
                    scroll_selection(self.mcp.selected, self.mcp.servers.len(), delta);
            }
            Tab::Schedules => {
                self.schedules.selected = scroll_selection(
                    self.schedules.selected,
                    self.schedules.schedules.len(),
                    delta,
                );
            }
            Tab::Skills => {
                self.skills.selected =
                    scroll_selection(self.skills.selected, self.skills.skills.len(), delta);
            }
        }
    }

    /// Scroll the chat transcript down by `delta` lines, clamped to
    /// `chat.max_scroll` (see its doc comment) so repeated presses past the
    /// bottom don't leave the opposite key needing an equal number of presses
    /// before the view visibly moves.
    fn chat_scroll_down(&mut self, delta: u16) {
        self.chat.auto_scroll = false;
        self.chat.scroll_offset = self
            .chat
            .scroll_offset
            .saturating_add(delta)
            .min(self.chat.max_scroll.get());
    }

    /// Scroll the chat transcript up by `delta` lines; naturally bounded at 0.
    fn chat_scroll_up(&mut self, delta: u16) {
        self.chat.auto_scroll = false;
        self.chat.scroll_offset = self.chat.scroll_offset.saturating_sub(delta);
    }

    /// `g`: jump to the top of the transcript.
    fn chat_scroll_top(&mut self) {
        self.chat.auto_scroll = false;
        self.chat.scroll_offset = 0;
    }

    /// `G`: jump to the bottom and resume auto-scroll.
    fn chat_scroll_bottom(&mut self) {
        self.chat.auto_scroll = true;
    }

    /// Scroll the raw config view down by `delta` lines, clamped to
    /// `config.max_scroll` — same rationale as `chat_scroll_down`.
    fn config_scroll_down(&mut self, delta: u16) {
        self.config.scroll_offset = self
            .config
            .scroll_offset
            .saturating_add(delta)
            .min(self.config.max_scroll.get());
    }

    /// Scroll the raw config view up by `delta` lines; naturally bounded at 0.
    fn config_scroll_up(&mut self, delta: u16) {
        self.config.scroll_offset = self.config.scroll_offset.saturating_sub(delta);
    }

    /// Whether an exclusive modal currently owns the keyboard. `F1`/Ctrl+`?`/
    /// Ctrl+L are gated on this so they can't stack a second overlay on top
    /// of one of these seven — see the precedence comment on `handle_key`.
    fn any_modal_open(&self) -> bool {
        self.serve_offer.is_some()
            || self.pending_question.is_some()
            || self.pending_delete.is_some()
            || self.session_picker.is_some()
            || self.model_picker.is_some()
            || self.schedule_form.is_some()
            || self.config_editor.is_some()
    }

    /// Route one key event.
    ///
    /// Modal precedence (checked top to bottom, each returning early — so
    /// exactly one visible modal owns the keyboard, and every one of them runs
    /// before the global bindings further down: Ctrl+N/Ctrl+O/Ctrl+Q, `?`,
    /// digit tab-switching, Tab/Shift+Tab):
    ///   0. `serve_offer`      — startup-only "start a local server?" offer
    ///   1. `pending_question` — agent permission/clarification gate
    ///   2. `pending_delete`   — session delete confirmation
    ///   3. `session_picker`   — Ctrl+P contextual session picker
    ///   4. `model_picker`     — Ctrl+O provider-catalog picker
    ///   5. `schedule_form`    — new-schedule authoring form
    ///   6. `config_editor`    — raw config JSON editor
    ///
    /// Runtime modals can coexist in state because a clarification arrives
    /// asynchronously. The renderer layers them in the reverse order below,
    /// keeping this keyboard owner visible without discarding editor drafts.
    ///
    /// `serve_offer` can never actually coexist with 1-5 in practice — `run`
    /// sets it (if at all) before the main loop starts driving any of the
    /// others, and it's always cleared before the loop's first redraw — but
    /// it is still checked first, for the same "whichever modal is open gets
    /// first refusal at every key" reason as the rest of this list.
    ///
    /// Ctrl+C (stop/quit) always preempts every modal. F1/Ctrl+`?`/Ctrl+L
    /// (help/notifications) are gated on `any_modal_open` below instead: with
    /// a modal open they fall straight through to that modal's own handler,
    /// so a stray F1 can't eat the keystroke the modal was waiting for or
    /// stack the help overlay on top of it.
    async fn handle_key(&mut self, key: KeyEvent) -> Result<()> {
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                if self.chat.streaming {
                    self.stop_streaming();
                    return Ok(());
                }
                self.running = false;
                return Ok(());
            }
            (KeyModifiers::CONTROL, KeyCode::Char('?')) if !self.any_modal_open() => {
                // Most terminals never deliver Ctrl+Shift+/ as Ctrl+'?' (it
                // maps elsewhere), so this is kept only as a harmless extra —
                // F1 below and plain `?` (further down, non-Chat tabs) are
                // the bindings that are actually reachable.
                self.help_visible = true;
                return Ok(());
            }
            (_, KeyCode::F(1)) if !self.any_modal_open() => {
                // F1 opens help on every tab, including Chat, regardless of
                // modifiers — unlike `?` it never collides with typing.
                self.help_visible = true;
                return Ok(());
            }
            (KeyModifiers::CONTROL, KeyCode::Char('l')) if !self.any_modal_open() => {
                self.notifications_visible = true;
                self.unseen_alerts = 0;
                return Ok(());
            }
            _ => {}
        }

        // 0. The startup "start a local server?" offer captures all input
        // (Ctrl+C above still quits) until answered — see `ServeOffer`.
        if self.serve_offer.is_some() {
            self.handle_serve_offer_key(key);
            return Ok(());
        }

        // 1. A pending agent question captures all input (Ctrl+C above still
        // stops the run) until it is answered or dismissed.
        if self.pending_question.is_some() {
            return self.handle_question_key(key).await;
        }

        // 2. The delete-confirmation modal likewise captures all input before
        // anything else (including tab-switching digits) can reach it.
        if self.pending_delete.is_some() {
            return self.handle_delete_confirm_key(key).await;
        }

        // 3. The contextual session picker owns search/navigation/mutations.
        if self.session_picker.is_some() {
            return self.handle_session_picker_key(key).await;
        }

        // 4. The model picker likewise captures all input (navigation/apply)
        // before the global bindings below — same pattern as the other
        // modals.
        if self.model_picker.is_some() {
            return self.handle_model_picker_key(key).await;
        }

        // 5. The schedule-authoring modal likewise captures all input: Tab moves
        // between fields and digits belong in cron expressions, so it must run
        // before the global Tab/1-6 tab-switching below (which would otherwise
        // swallow those keys and never reach the form).
        if self.schedule_form.is_some() {
            self.handle_schedule_form_key(key);
            return Ok(());
        }

        // 6. The config editor is a full multi-line text buffer, so it must claim
        // every key (digits, Tab, Enter/newlines) before the global navigation
        // below — same rationale as the schedule form.
        if self.config_editor.is_some() {
            return self.handle_config_editor_key(key);
        }

        // Ctrl+N / Ctrl+Q: global, but only reachable once every modal above
        // has had first refusal at the key (each of those branches returns
        // early) — so getting here already means no modal is open, matching
        // the "no modal open" requirement for Ctrl+N specifically.
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('n') if !self.chat.streaming => {
                    self.new_session();
                    return Ok(());
                }
                KeyCode::Char('q') => {
                    self.reopen_pending_question();
                    return Ok(());
                }
                KeyCode::Char('o') if self.tab == Tab::Chat && !self.chat.streaming => {
                    self.open_model_picker();
                    return Ok(());
                }
                KeyCode::Char('p') if self.tab == Tab::Chat && !self.chat.streaming => {
                    self.open_session_picker();
                    return Ok(());
                }
                _ => {}
            }
        }

        // `?` opens help everywhere EXCEPT Chat, where it must type into the
        // textarea instead (mirrors the digit rule right below: Chat's
        // textarea wins over global single-key bindings). F1 above is the
        // Chat-safe way to reach help.
        if key.code == KeyCode::Char('?') && self.tab != Tab::Chat {
            self.help_visible = true;
            return Ok(());
        }

        // 1-6 switch tabs EXCEPT on Chat, where digits must type into the
        // message instead — otherwise typing e.g. "top 3 issues" silently
        // jumps to the Config tab on the '3'. Shift+digit (many keyboards'
        // symbol row) never switches tabs, matching the pre-existing rule.
        if let KeyCode::Char(c) = key.code {
            if let Some(digit) = c.to_digit(10) {
                if (1..=6).contains(&digit)
                    && !key.modifiers.contains(KeyModifiers::SHIFT)
                    && self.tab != Tab::Chat
                {
                    self.tab = Tab::from_index((digit - 1) as usize).unwrap_or(self.tab);
                    self.load_tab_data();
                    return Ok(());
                }
            }
        }

        match key.code {
            KeyCode::Tab => {
                self.tab = self.tab.next();
                self.load_tab_data();
            }
            KeyCode::BackTab => {
                self.tab = self.tab.prev();
                self.load_tab_data();
            }
            _ => {
                self.handle_tab_key(key).await?;
            }
        }
        Ok(())
    }

    /// Drive the startup "start a local server?" offer (`ServeOffer`):
    /// `y`/Enter starts the spawn flow (`spawn_local_server`); `n`/Esc
    /// dismisses without spawning, leaving the operator to restart with
    /// `--auto-serve` or start `bamboo serve` themselves. Every other key is
    /// swallowed — see the modal-precedence doc on `handle_key`.
    fn handle_serve_offer_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.serve_offer = None;
                self.spawn_local_server();
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                self.serve_offer = None;
                self.status_message =
                    "Not connected — restart with --auto-serve or start 'bamboo serve' yourself"
                        .to_string();
            }
            _ => {}
        }
    }

    /// Resolve the `bamboo` binary and spawn `bamboo serve` as an auto-serve
    /// child. Everything here is off the UI loop except the actual
    /// `Command::spawn()` call, which is synchronous but non-blocking (it
    /// forks/execs and returns immediately, it doesn't wait on the child) —
    /// only the health-poll wait *after* the spawn is a `tokio::spawn`'d
    /// task. The `Child` is created and stored here (not inside that task)
    /// so it lands in `self.spawned_server` — and hence gets `kill_on_drop`
    /// protection — before this function returns, rather than floating in a
    /// detached task with no owner in the meantime.
    fn spawn_local_server(&mut self) {
        let Some(bin) = discover_bamboo_bin(
            std::env::var_os("BAMBOO_BIN").map(PathBuf::from),
            std::env::current_exe()
                .ok()
                .as_deref()
                .and_then(Path::parent),
            std::env::var("PATH").ok().as_deref(),
        ) else {
            self.notify(
                NoticeLevel::Error,
                "Can't find a `bamboo` binary to auto-start — set BAMBOO_BIN or add it to PATH",
            );
            return;
        };

        // `serve`'s stdout is NOT quiet (tracing_subscriber fmt lines plus a
        // raw `println!`) — inheriting it here would corrupt this
        // alternate-screen TUI, so both streams are redirected to a log file
        // instead (mirrors bodhi's sidecar, which drains and re-logs rather
        // than inheriting).
        let log_path = std::env::temp_dir().join("bamboo-tui-server.log");
        let (stdout_file, stderr_file) = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .and_then(|f| Ok((f.try_clone()?, f)))
        {
            Ok(files) => files,
            Err(e) => {
                self.notify(
                    NoticeLevel::Error,
                    format!("Failed to open server log {}: {e}", log_path.display()),
                );
                return;
            }
        };

        let mut args = vec![
            "serve".to_string(),
            // Crash-safe orphan guard: `serve` exits if this TUI process
            // disappears without running any cleanup (SIGKILL) — the second
            // line of defense behind `spawned_server`'s `kill_on_drop`.
            "--parent-pid".to_string(),
            std::process::id().to_string(),
        ];
        let (host, port) = parse_authority(&self.client.base_url);
        if let Some(port) = port {
            if port != DEFAULT_SERVER_PORT {
                args.push("--port".to_string());
                args.push(port.to_string());
                args.push("--bind".to_string());
                args.push(if host.is_empty() {
                    DEFAULT_SERVER_BIND.to_string()
                } else {
                    host
                });
            }
        }

        let mut command = Command::new(&bin);
        command
            .args(&args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(stdout_file))
            .stderr(std::process::Stdio::from(stderr_file))
            .kill_on_drop(true);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                self.notify(
                    NoticeLevel::Error,
                    format!(
                        "Failed to spawn {} serve: {e} (log: {})",
                        bin.display(),
                        log_path.display()
                    ),
                );
                return;
            }
        };
        // `Child::id()` only returns `None` once the child has already been
        // reaped (`wait`ed on) — never true immediately after a successful
        // `spawn()`, so `0` here is unreachable in practice, not a silently
        // wrong pid.
        let pid = child.id().unwrap_or(0);

        let Some(tx) = self.event_tx.clone() else {
            // No event loop to report back to (shouldn't happen — `run` sets
            // this before the health check that leads here) — best-effort
            // kill so an orphaned child isn't left behind either.
            let _ = child.start_kill();
            return;
        };
        self.spawned_server = Some(child);
        self.status_message = "Starting local server...".to_string();

        let client = self.client.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
            loop {
                if matches!(client.health().await, Ok(true)) {
                    let _ = tx.send(AppEvent::LocalServerReady(Ok(pid)));
                    return;
                }
                if tokio::time::Instant::now() >= deadline {
                    let _ = tx.send(AppEvent::LocalServerReady(Err(format!(
                        "local server did not become healthy within 20s (log: {})",
                        log_path.display()
                    ))));
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        });
    }

    /// Drive the pending-question modal. Returns an answer to submit (if the
    /// keystroke commits one) without holding a borrow across the async submit.
    async fn handle_question_key(&mut self, key: KeyEvent) -> Result<()> {
        enum QAction {
            None,
            Dismiss,
            CloseCustom,
            Submit(String),
            Copy(String),
        }

        let action = {
            let Some(q) = self.pending_question.as_mut() else {
                return Ok(());
            };
            if q.submitting {
                // An answer POST is in flight: swallow every key. This both
                // prevents a double-submit on repeated Enter and keeps the
                // question from being dismissed/mutated out from under the
                // request (Ctrl+C at the top of `handle_key` still preempts).
                return Ok(());
            }

            if q.inspecting {
                match key.code {
                    KeyCode::Char('v') | KeyCode::Esc => {
                        q.inspecting = false;
                        q.inspect_scroll = 0;
                        QAction::None
                    }
                    KeyCode::Tab if !q.options.is_empty() => {
                        q.inspect_option = !q.inspect_option;
                        q.inspect_scroll = 0;
                        QAction::None
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        q.inspect_scroll = q
                            .inspect_scroll
                            .min(q.inspect_max_scroll.get())
                            .saturating_sub(1);
                        QAction::None
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        q.inspect_scroll = q
                            .inspect_scroll
                            .min(q.inspect_max_scroll.get())
                            .saturating_add(1)
                            .min(q.inspect_max_scroll.get());
                        QAction::None
                    }
                    KeyCode::PageUp => {
                        q.inspect_scroll = q
                            .inspect_scroll
                            .min(q.inspect_max_scroll.get())
                            .saturating_sub(5);
                        QAction::None
                    }
                    KeyCode::PageDown => {
                        q.inspect_scroll = q
                            .inspect_scroll
                            .min(q.inspect_max_scroll.get())
                            .saturating_add(5)
                            .min(q.inspect_max_scroll.get());
                        QAction::None
                    }
                    KeyCode::Home => {
                        q.inspect_scroll = 0;
                        QAction::None
                    }
                    KeyCode::Char('y') => {
                        let value = if q.inspect_option {
                            q.options.get(q.selected).cloned().unwrap_or_default()
                        } else {
                            q.question.clone()
                        };
                        QAction::Copy(value)
                    }
                    _ => QAction::None,
                }
            } else if let Some(entry) = q.number_entry.as_mut() {
                match key.code {
                    KeyCode::Char(d) if d.is_ascii_digit() => {
                        entry.push(d);
                        QAction::None
                    }
                    KeyCode::Backspace => {
                        entry.pop();
                        QAction::None
                    }
                    KeyCode::Esc => {
                        q.number_entry = None;
                        QAction::None
                    }
                    KeyCode::Enter => {
                        let requested = entry.parse::<usize>().ok();
                        q.number_entry = None;
                        match requested.and_then(|number| number.checked_sub(1)) {
                            Some(index) if index < q.options.len() => {
                                q.selected = index;
                                q.error = None;
                            }
                            _ => {
                                q.error = Some("That option number does not exist".to_string());
                            }
                        }
                        QAction::None
                    }
                    _ => QAction::None,
                }
            } else if let Some(buf) = q.custom.as_mut() {
                // Free-text entry mode.
                match key.code {
                    KeyCode::Enter => {
                        if buf.trim().is_empty() {
                            QAction::None
                        } else {
                            QAction::Submit(buf.clone())
                        }
                    }
                    KeyCode::Esc => {
                        // Back to option-select if there were options, else dismiss.
                        if q.options.is_empty() {
                            QAction::Dismiss
                        } else {
                            QAction::CloseCustom
                        }
                    }
                    KeyCode::Backspace => {
                        buf.pop();
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::Char('v') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        q.inspecting = true;
                        q.inspect_option = false;
                        q.inspect_scroll = 0;
                        QAction::None
                    }
                    KeyCode::Char(c) => {
                        buf.push(c);
                        q.error = None;
                        QAction::None
                    }
                    _ => QAction::None,
                }
            } else {
                // Option-select mode.
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        q.selected = q.selected.saturating_sub(1);
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if q.selected + 1 < q.options.len() {
                            q.selected += 1;
                        }
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::PageUp => {
                        q.selected = q.selected.saturating_sub(5);
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::PageDown => {
                        q.selected = q
                            .selected
                            .saturating_add(5)
                            .min(q.options.len().saturating_sub(1));
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::Home => {
                        q.selected = 0;
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::End => {
                        q.selected = q.options.len().saturating_sub(1);
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::Char('c') if q.allow_custom => {
                        // Switch to free-text entry without discarding a draft
                        // entered before dismissal/session switching.
                        q.custom = Some(std::mem::take(&mut q.custom_draft));
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::Char('g') if !q.options.is_empty() => {
                        q.number_entry = Some(String::new());
                        q.error = None;
                        QAction::None
                    }
                    KeyCode::Char('v') => {
                        q.inspecting = true;
                        q.inspect_option = false;
                        q.inspect_scroll = 0;
                        QAction::None
                    }
                    KeyCode::Char('y') => {
                        let value = q
                            .options
                            .get(q.selected)
                            .cloned()
                            .unwrap_or_else(|| q.question.clone());
                        QAction::Copy(value)
                    }
                    KeyCode::Enter => q
                        .options
                        .get(q.selected)
                        .cloned()
                        .map(QAction::Submit)
                        .unwrap_or(QAction::None),
                    KeyCode::Char(d) if ('1'..='9').contains(&d) => {
                        let idx = d as usize - '1' as usize;
                        q.options
                            .get(idx)
                            .cloned()
                            .map(QAction::Submit)
                            .unwrap_or(QAction::None)
                    }
                    KeyCode::Esc => QAction::Dismiss,
                    _ => QAction::None,
                }
            }
        };

        match action {
            QAction::Submit(answer) => self.submit_answer(answer),
            QAction::CloseCustom => {
                if let Some(question) = self.pending_question.as_mut() {
                    question.custom_draft = question.custom.take().unwrap_or_default();
                }
            }
            QAction::Copy(value) => match copy_via_osc52(&value) {
                Ok(()) => self.status_message = "Copied exact text".to_string(),
                Err(error) => {
                    self.notify(NoticeLevel::Error, format!("Failed to copy text: {error}"))
                }
            },
            QAction::Dismiss => {
                // Keep it, not just drop it: dismissing does NOT tell the
                // server to stop waiting, so the run is still blocked on an
                // answer — Ctrl+Q brings the modal back without a round-trip.
                self.supersede_pending_answer();
                self.dismissed_question = self.pending_question.take();
                self.status_message = "Question dismissed (still pending on the server — \
                    Ctrl+Q to reopen, Ctrl+C stops the run)"
                    .to_string();
            }
            QAction::None => {}
        }
        Ok(())
    }

    fn stash_question_drafts(&mut self) {
        let drafts = [
            self.pending_question.as_ref(),
            self.dismissed_question.as_ref(),
        ]
        .into_iter()
        .flatten()
        .filter(|question| question.allow_custom)
        .map(|question| (question.draft_key(), question.draft().to_string()))
        .collect::<Vec<_>>();
        for (key, draft) in drafts {
            self.store_question_draft(key, draft);
        }
    }

    fn store_question_draft(&mut self, key: QuestionDraftKey, draft: String) {
        self.question_draft_order
            .retain(|existing| existing != &key);
        if draft.is_empty() {
            self.question_drafts.remove(&key);
            return;
        }

        self.question_drafts.insert(key.clone(), draft);
        self.question_draft_order.push_back(key);
        while self.question_draft_order.len() > MAX_QUESTION_DRAFTS {
            if let Some(oldest) = self.question_draft_order.pop_front() {
                self.question_drafts.remove(&oldest);
            }
        }
    }

    fn take_question_draft(&mut self, key: &QuestionDraftKey) -> Option<String> {
        let draft = self.question_drafts.remove(key);
        if draft.is_some() {
            self.question_draft_order.retain(|existing| existing != key);
        }
        draft
    }

    fn remove_question_drafts_for_identity(&mut self, identity: &QuestionIdentity) {
        self.question_drafts
            .retain(|key, _| &key.identity != identity);
        self.question_draft_order
            .retain(|key| &key.identity != identity);
    }

    fn question_from_pending(
        &mut self,
        session_id: String,
        pending: &PendingQuestion,
    ) -> ActiveQuestion {
        let exact_key = QuestionDraftKey::new(
            session_id.clone(),
            pending.tool_call_id.clone(),
            pending.question.clone(),
            pending.options.clone().unwrap_or_default(),
            pending.allow_custom,
        );
        let draft = if pending.allow_custom {
            self.take_question_draft(&exact_key).or_else(|| {
                pending.tool_call_id.as_ref()?;
                let legacy_key = QuestionDraftKey::new(
                    session_id.clone(),
                    None,
                    pending.question.clone(),
                    pending.options.clone().unwrap_or_default(),
                    pending.allow_custom,
                );
                self.take_question_draft(&legacy_key)
            })
        } else {
            None
        }
        .unwrap_or_default();
        ActiveQuestion::from_pending(session_id, pending, draft)
    }

    /// Invalidate any in-flight answer POST by bumping `answer_epoch`. Called
    /// from every site that changes the pending-question context (a new
    /// question arriving, a session switch/resume, the run finalizing, the
    /// modal being dismissed or reopened) so that a late
    /// `AppEvent::AnswerSubmitted` carrying an older epoch is discarded in
    /// `handle_event` instead of applied to a question it doesn't belong to.
    fn supersede_pending_answer(&mut self) {
        if let Some(task) = self.answer_task.take() {
            task.abort();
        }
        self.answer_epoch = self.answer_epoch.wrapping_add(1);
        self.pending_answer_run_started = false;
    }

    /// Submit an answer to the agent's pending question WITHOUT blocking the
    /// event loop: the `respond` POST is spawned off the UI thread (this used
    /// to be awaited inline inside `handle_event`, freezing every redraw/key/
    /// SSE drain until the server replied) and its outcome comes back as
    /// `AppEvent::AnswerSubmitted`. Until then the modal stays open in a
    /// "Submitting answer…" state with input disabled.
    fn submit_answer(&mut self, answer: String) {
        let Some(identity) = self.pending_question.as_ref().map(ActiveQuestion::identity) else {
            return;
        };
        if self.chat.session_id.as_deref() != Some(identity.session_id.as_str()) {
            self.notify(
                NoticeLevel::Warn,
                "Question belongs to a different session; reopen that session before answering",
            );
            return;
        }
        if identity.tool_call_id.is_none() {
            if let Some(question) = self.pending_question.as_mut() {
                question.identity_syncing = true;
                question.error = Some(
                    "Waiting for the server's exact question identity; answer not sent".to_string(),
                );
            }
            self.status_message = "Synchronizing question identity...".to_string();
            self.reconcile_pending_question_after_stream_connect(identity.session_id);
            return;
        }
        // The old pause activation can have queued its terminal Complete while
        // its SSE task/readiness flag still looks live. Reusing that generation
        // creates a gap: the server closes it at Complete and a fast successor
        // run can finish before the UI processes Complete and reconnects.
        // Always install and await a fresh subscriber generation before POSTing
        // the answer; attaching also discards any queued terminal from the old
        // pause stream.
        if !self.attach_stream(identity.session_id.clone()) {
            if let Some(question) = self.pending_question.as_mut() {
                question.error = Some("Could not attach the event stream; answer not sent".into());
            }
            return;
        }
        let Some(mut sse_ready) = self.sse_ready.clone() else {
            if let Some(question) = self.pending_question.as_mut() {
                question.error = Some("Event stream is unavailable; answer not sent".into());
            }
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let Some(q) = self.pending_question.as_mut() else {
            return;
        };
        if q.submitting {
            // Belt-and-braces double-submit guard: `handle_question_key`
            // already swallows every key while in flight.
            return;
        }
        q.submitting = true;
        q.error = None;
        self.pending_answer_run_started = false;

        // Claim a fresh epoch for this submission; the spawned task carries a
        // copy so the handler can tell whether the response still belongs to
        // the current question when it lands.
        self.supersede_pending_answer();
        let epoch = self.answer_epoch;
        self.status_message = format!("Submitting answer: {answer}…");

        let client = self.client.clone();
        let session_id = identity.session_id.clone();
        let expected_tool_call_id = identity.tool_call_id.clone();
        let submitted_identity = identity.clone();
        self.answer_task = Some(tokio::spawn(async move {
            let stream_ready = if *sse_ready.borrow() {
                Ok(())
            } else {
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    sse_ready.wait_for(|ready| *ready),
                )
                .await
                .map_err(|_| "timed out waiting for the event stream".to_string())
                .and_then(|result| {
                    result
                        .map(|_| ())
                        .map_err(|_| "event stream closed before subscribing".to_string())
                })
            };
            let result = match stream_ready {
                Ok(()) => {
                    client
                        .respond(&session_id, &answer, expected_tool_call_id.as_deref())
                        .await
                }
                Err(message) => Err(RespondFailure::unavailable(format!(
                    "Answer not sent: {message}"
                ))),
            };
            let _ = tx.send(AppEvent::AnswerSubmitted {
                epoch,
                identity: submitted_identity,
                answer,
                result,
            });
        }));
    }

    fn refresh_question_after_rejection(&mut self, identity: QuestionIdentity, error: String) {
        let Some(tx) = self.event_tx.clone() else {
            if let Some(question) = self.pending_question.as_mut() {
                question.submitting = false;
                question.error = Some(error);
            }
            return;
        };
        if let Some(question) = self.pending_question.as_mut() {
            // Keep input disabled until the authoritative pending state is
            // known; retrying during reconciliation could repeat the same
            // stale submission.
            question.error = Some(format!("{error}; refreshing from server..."));
        }
        self.supersede_pending_answer();
        let epoch = self.answer_epoch;
        let session_id = identity.session_id.clone();
        let client = self.client.clone();
        tokio::spawn(async move {
            let result = client
                .get_pending_question(&session_id)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(AppEvent::PendingQuestionRefreshed {
                session_id,
                epoch,
                identity,
                result,
            });
        });
    }

    /// Drive the delete-confirmation modal (`d` on the Sessions tab). `y`/Enter
    /// confirms and spawns the DELETE off the event loop (never `?` on the UI
    /// thread — a failed delete must surface via `notify`, not tear down the
    /// TUI); `n`/Esc cancels without calling the server.
    async fn handle_delete_confirm_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some((id, _title)) = self.pending_delete.clone() else {
            return Ok(());
        };
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.pending_delete = None;
                if self.deleting_session_id.is_some() {
                    self.notify(
                        NoticeLevel::Warn,
                        "Wait for the current session delete to finish",
                    );
                    return Ok(());
                }
                if self.chat.streaming && self.chat.session_id.as_deref() == Some(id.as_str()) {
                    self.notify(
                        NoticeLevel::Warn,
                        "Stop the active run before deleting its session",
                    );
                    return Ok(());
                }
                let Some(tx) = self.event_tx.clone() else {
                    self.notify(
                        NoticeLevel::Error,
                        "Session delete is not attached to an event loop",
                    );
                    return Ok(());
                };
                self.deleting_session_id = Some(id.clone());
                self.status_message = "Deleting session...".to_string();
                let client = self.client.clone();
                let session_picker_epoch = self.session_picker.as_ref().map(|picker| picker.epoch);
                tokio::spawn(async move {
                    let result = client
                        .delete_session(&id)
                        .await
                        .map_err(|error| error.to_string());
                    let _ = tx.send(AppEvent::SessionDeleted {
                        session_id: id,
                        result,
                        session_picker_epoch,
                    });
                });
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                self.pending_delete = None;
            }
            _ => {}
        }
        Ok(())
    }

    /// Load the current tab's data WITHOUT blocking the event loop: mark the
    /// tab loading and spawn the fetch, which posts its result back as an
    /// `AppEvent` handled in `handle_event`.
    fn load_tab_data(&mut self) {
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        match self.tab {
            Tab::Chat => {}
            Tab::Sessions => {
                self.sessions.loading = true;
                let offset = self.sessions.offset;
                // `0` means "no page size established yet" — let the server
                // pick its own bounded default rather than guessing one here.
                let limit = (self.sessions.page_limit > 0).then_some(self.sessions.page_limit);
                tokio::spawn(async move {
                    let r = client
                        .list_sessions(limit, Some(offset))
                        .await
                        .map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::SessionsLoaded(r));
                });
            }
            Tab::Mcp => {
                self.mcp.loading = true;
                tokio::spawn(async move {
                    let r = client.list_mcp_servers().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::McpServersLoaded(r));
                });
            }
            Tab::Schedules => {
                self.schedules.loading = true;
                tokio::spawn(async move {
                    let r = client.list_schedules().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::SchedulesLoaded(r));
                });
            }
            Tab::Skills => {
                self.skills.loading = true;
                tokio::spawn(async move {
                    let r = client.list_skills().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::SkillsLoaded(r));
                });
            }
            Tab::Config => {
                self.config.loading = true;
                tokio::spawn(async move {
                    let r = client.get_config().await.map_err(|e| e.to_string());
                    let _ = tx.send(AppEvent::ConfigLoaded(r));
                });
            }
        }
    }

    async fn handle_tab_key(&mut self, key: KeyEvent) -> Result<()> {
        match self.tab {
            Tab::Chat => self.handle_chat_key(key).await?,
            Tab::Sessions => self.handle_sessions_key(key).await?,
            Tab::Mcp => self.handle_mcp_key(key).await?,
            Tab::Schedules => self.handle_schedules_key(key).await?,
            Tab::Skills => self.handle_skills_key(key).await?,
            Tab::Config => self.handle_config_key(key),
        }
        Ok(())
    }

    async fn handle_chat_key(&mut self, key: KeyEvent) -> Result<()> {
        if self.chat.streaming {
            match key.code {
                KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.stop_streaming();
                }
                KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.chat.expand_tools = !self.chat.expand_tools;
                }
                KeyCode::Char('j') | KeyCode::Down => self.chat_scroll_down(3),
                KeyCode::Char('k') | KeyCode::Up => self.chat_scroll_up(3),
                KeyCode::PageDown => self.chat_scroll_down(10),
                KeyCode::PageUp => self.chat_scroll_up(10),
                KeyCode::Char('g') => self.chat_scroll_top(),
                KeyCode::Char('G') => self.chat_scroll_bottom(),
                _ => {}
            }
            return Ok(());
        }

        match key.code {
            // Alt+Enter (and Shift+Enter, on the kitty-protocol terminals
            // that report it — plain crossterm terminals mostly don't, so
            // this arm is harmless there) inserts a newline instead of
            // sending, since plain Enter always sends.
            KeyCode::Enter
                if key.modifiers.contains(KeyModifiers::ALT)
                    || key.modifiers.contains(KeyModifiers::SHIFT) =>
            {
                self.chat.textarea.insert_newline();
            }
            KeyCode::Enter => {
                // Selecting a session starts an asynchronous history/summary
                // fetch after the picker closes. Keep the existing draft
                // editable while that fetch is in flight, but never submit it
                // against the session still visible underneath: a concurrent
                // `ChatStarted` and `SessionOpened` could otherwise route the
                // turn (or its provider/model) to different sessions.
                if self.opening_session_id.is_some() {
                    self.status_message =
                        "Session is still resuming — message kept as draft".to_string();
                    return Ok(());
                }
                if self.chat.session_id.as_ref().is_some_and(|session_id| {
                    self.deleting_session_id.as_deref() == Some(session_id.as_str())
                }) {
                    self.status_message =
                        "Session is being deleted — message kept as draft".to_string();
                    return Ok(());
                }
                let input = self.chat.textarea.lines().join("\n");
                let input = input.trim().to_string();
                if input.is_empty() {
                    return Ok(());
                }
                self.chat.textarea = TextArea::default();
                self.chat.textarea.set_placeholder_text(CHAT_PLACEHOLDER);
                self.send_message(input);
            }
            KeyCode::Char('j') | KeyCode::Down => self.chat_scroll_down(3),
            KeyCode::Char('k') | KeyCode::Up => self.chat_scroll_up(3),
            KeyCode::PageDown => self.chat_scroll_down(10),
            KeyCode::PageUp => self.chat_scroll_up(10),
            KeyCode::Char('g') => self.chat_scroll_top(),
            KeyCode::Char('G') => self.chat_scroll_bottom(),
            KeyCode::Char('x') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.chat.expand_tools = !self.chat.expand_tools;
            }
            _ => {
                self.chat.textarea.input(key);
            }
        }
        Ok(())
    }

    /// Send a chat message WITHOUT blocking the event loop: the user turn is
    /// shown immediately (optimistic), and the `chat` POST runs on a task that
    /// posts `ChatStarted` back — the handler then opens the SSE stream and
    /// spawns `execute` once the session id is known.
    fn send_message(&mut self, message: String) {
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let model = if self.chat.model.is_empty() {
            "default".to_string()
        } else {
            self.chat.model.clone()
        };

        // Optimistic UI: show the user's turn and switch to streaming right away.
        self.chat.messages.push(ChatMessage {
            role: MessageRole::User,
            content: message.clone(),
            tool_calls: Vec::new(),
            reasoning: None,
        });
        self.chat.auto_scroll = true;
        self.chat.streaming = true;
        self.chat.current_response.clear();
        self.chat.current_tool_calls.clear();
        self.chat.current_reasoning.clear();
        self.status_message = "Sending...".to_string();

        let client = self.client.clone();
        let existing_session = self.chat.session_id.clone();
        let project_id = self.chat.project_id.clone();
        let provider = self.chat.provider.clone();
        tokio::spawn(async move {
            let req = ChatRequest {
                message,
                session_id: existing_session,
                project_id,
                model: Some(model),
                provider,
            };
            let result = client
                .chat(req)
                .await
                .map(|resp| resp.session_id)
                .map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::ChatStarted(result));
        });
    }

    /// Open the SSE stream for `session_id`, wiring `sse_tx`/`sse_rx` so
    /// `poll_sse`/`run`'s select loop starts receiving events. Shared by a
    /// freshly-started run (`start_stream_and_execute`, before `execute` so no
    /// early event is missed) and reattaching to an already-running session on
    /// resume (`AppEvent::SessionOpened` with `is_running: true`). Successful
    /// retries also trigger an authoritative pending-question reconciliation,
    /// because that state is not part of the server's critical-event replay.
    /// Returns whether the connection was opened; on failure it has already
    /// `notify`'d.
    fn attach_stream(&mut self, session_id: String) -> bool {
        self.detach_stream();
        let stream_epoch = self.sse_epoch;
        let (sse_tx, sse_rx) = mpsc::unbounded_channel();
        self.sse_tx = Some(sse_tx.clone());
        self.sse_rx = Some(sse_rx);
        self.stream_disconnected = true;
        let base_url = self.client.base_url.clone();
        match SseStream::start(&base_url, &session_id, stream_epoch, sse_tx) {
            Ok((task, ready)) => {
                self.sse_task = Some(task);
                self.sse_ready = Some(ready);
            }
            Err(error) => {
                self.detach_stream();
                self.notify(NoticeLevel::Error, format!("SSE start failed: {error}"));
                return false;
            }
        }
        true
    }

    fn stream_is_ready(&self) -> bool {
        self.sse_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
            && self.sse_ready.as_ref().is_some_and(|ready| *ready.borrow())
    }

    fn detach_stream(&mut self) {
        self.sse_epoch = self.sse_epoch.wrapping_add(1);
        self.pending_reconcile_epoch = self.pending_reconcile_epoch.wrapping_add(1);
        if let Some(task) = self.sse_task.take() {
            task.abort();
        }
        self.sse_ready = None;
        self.sse_tx = None;
        self.sse_rx = None;
    }

    /// After `chat` returns a session id, open the SSE stream (before execute, so
    /// no early event is missed) and spawn the agent run.
    fn start_stream_and_execute(&mut self, session_id: String) {
        if !self.attach_stream(session_id.clone()) {
            return;
        }
        let Some(mut sse_ready) = self.sse_ready.clone() else {
            self.notify(
                NoticeLevel::Error,
                "SSE readiness channel unavailable; run was not started",
            );
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        let model = self.chat.model.clone();
        let provider = self.chat.provider.clone();
        tokio::spawn(async move {
            let model = if model.is_empty() { None } else { Some(model) };
            let ready = if *sse_ready.borrow() {
                Ok(())
            } else {
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    sse_ready.wait_for(|ready| *ready),
                )
                .await
                .map_err(|_| "timed out waiting for the event stream".to_string())
                .and_then(|result| {
                    result
                        .map(|_| ())
                        .map_err(|_| "event stream closed before subscribing".to_string())
                })
            };
            if let Err(error) = ready {
                let _ = tx.send(AppEvent::ExecuteFailed(format!(
                    "execute not started: {error}"
                )));
                return;
            }
            // If this POST fails (server down, 4xx/5xx), no SSE terminal event
            // will ever arrive for a run that never started — report it back so
            // the handler can finalize `chat.streaming` instead of spinning
            // forever waiting for events behind a run that doesn't exist.
            if let Err(e) = client
                .execute(&session_id, model.as_deref(), provider.as_deref())
                .await
            {
                let _ = tx.send(AppEvent::ExecuteFailed(e.to_string()));
            }
        });
    }

    /// Resume a session fully off the event loop: fetch its history + summary
    /// (+ pending question, when the summary reports one), map the history
    /// into chat messages, and post a single `SessionOpened` event. Used both
    /// by `Enter` on the Sessions tab and by `--session-id` at startup so
    /// there's exactly one resume code path.
    fn resume_session(&mut self, session_id: String) {
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        self.opening_session_id = Some(session_id.clone());
        let client = self.client.clone();
        self.status_message = "Resuming session...".to_string();
        tokio::spawn(async move {
            let history = match client.get_history(&session_id).await {
                Ok(h) => h,
                Err(e) => {
                    let _ = tx.send(AppEvent::SessionOpened {
                        session_id,
                        result: Err(e.to_string()),
                    });
                    return;
                }
            };
            let summary = match client.get_session(&session_id).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(AppEvent::SessionOpened {
                        session_id,
                        result: Err(e.to_string()),
                    });
                    return;
                }
            };
            // Only fetch the pending question when the summary says there is
            // one. A detail-fetch failure is fatal to this open attempt: the
            // summary is authoritative that input must remain blocked, so
            // opening without the modal would silently expose an unusable
            // chat composer and lose the discoverable retry path.
            let pending = if summary.has_pending_question {
                match client.get_pending_question(&session_id).await {
                    Ok(pending) => pending.has_pending_question.then_some(pending),
                    Err(error) => {
                        let _ = tx.send(AppEvent::SessionOpened {
                            session_id,
                            result: Err(format!(
                                "session has a pending question, but its details could not be loaded: {error}"
                            )),
                        });
                        return;
                    }
                }
            } else {
                None
            };
            let provider = summary
                .model_ref
                .as_ref()
                .map(|model_ref| model_ref.provider.clone())
                .or_else(|| summary.provider.clone());
            let opened = OpenedSession {
                messages: map_history(history.messages),
                model: summary.model,
                provider,
                project_id: summary.project_id,
                is_running: summary.is_running,
                pending,
                truncated: history.truncated,
                total_message_count: history.total_message_count,
            };
            let _ = tx.send(AppEvent::SessionOpened {
                session_id,
                result: Ok(opened),
            });
        });
    }

    /// `Ctrl+N`: start a fresh session. Keeps the current model and Project
    /// membership (the operator picked both deliberately) while dropping
    /// per-session conversation state.
    fn new_session(&mut self) {
        self.stash_question_drafts();
        self.detach_stream();
        self.opening_session_id = None;
        self.chat.session_id = None;
        self.chat.messages.clear();
        self.chat.current_response.clear();
        self.chat.current_tool_calls.clear();
        self.chat.current_reasoning.clear();
        self.chat.sub_agents.clear();
        self.chat.token_usage = None;
        self.chat.scroll_offset = 0;
        self.chat.auto_scroll = true;
        self.chat.streaming = false;
        self.chat.plan_mode = false;
        self.stream_disconnected = false;
        self.pending_answer_run_started = false;
        self.supersede_pending_answer();
        self.pending_question = None;
        self.dismissed_question = None;
        self.status_message = "New session".to_string();
    }

    /// `Ctrl+Q`: restore the last dismissed question if one is cached; else,
    /// if there's an active session, check the server off the event loop (the
    /// result comes back as `AppEvent::PendingQuestionChecked`).
    fn reopen_pending_question(&mut self) {
        if let Some(q) = self.dismissed_question.take() {
            // Reopening counts as a new question context too: any answer
            // somehow still in flight for the pre-dismissal modal must not
            // land on the reopened one.
            self.supersede_pending_answer();
            self.pending_question = Some(q);
            self.status_message = "Question reopened".to_string();
            return;
        }
        let Some(session_id) = self.chat.session_id.clone() else {
            self.notify(NoticeLevel::Info, "No pending question to reopen");
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        self.supersede_pending_answer();
        let epoch = self.answer_epoch;
        let client = self.client.clone();
        self.status_message = "Checking for a pending question...".to_string();
        tokio::spawn(async move {
            let r = client
                .get_pending_question(&session_id)
                .await
                .map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::PendingQuestionChecked {
                session_id,
                epoch,
                result: r,
            });
        });
    }

    fn reconcile_pending_question_after_stream_connect(&mut self, session_id: String) {
        if self.chat.session_id.as_deref() != Some(session_id.as_str()) {
            return;
        }
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let epoch = self.answer_epoch;
        self.pending_reconcile_epoch = self.pending_reconcile_epoch.wrapping_add(1);
        let reconcile_epoch = self.pending_reconcile_epoch;
        let client = self.client.clone();
        tokio::spawn(async move {
            let result = client
                .get_pending_question(&session_id)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(AppEvent::PendingQuestionReconciled {
                session_id,
                epoch,
                reconcile_epoch,
                result,
            });
        });
    }

    /// Find the in-progress tool call matching `tool_call_id`. Tool events are
    /// paired by this server-assigned id rather than list position/name so
    /// that parallel tool calls (multiple in-flight at once) each get their
    /// own Complete/Error/Lifecycle update instead of clobbering whichever
    /// entry happens to be last in the list.
    fn find_tool_mut(&mut self, tool_call_id: &str) -> Option<&mut ToolCallDisplay> {
        self.chat
            .current_tool_calls
            .iter_mut()
            .find(|t| t.id == tool_call_id)
    }

    fn handle_session_sse_event(&mut self, message: SessionSseEvent) -> Result<()> {
        match message {
            SessionSseEvent::Event {
                session_id,
                stream_epoch,
                event,
            } => {
                if self.chat.session_id.as_deref() != Some(session_id.as_str())
                    || self.sse_epoch != stream_epoch
                {
                    return Ok(());
                }
                self.handle_sse_event(event)
            }
            SessionSseEvent::Connected {
                session_id,
                stream_epoch,
                reconnecting,
            } => {
                if self.chat.session_id.as_deref() != Some(session_id.as_str())
                    || self.sse_epoch != stream_epoch
                {
                    return Ok(());
                }
                self.connected = true;
                self.stream_disconnected = false;
                if reconnecting {
                    self.status_message = "SSE reconnected — synchronizing".to_string();
                }
                // Answer submission deliberately attaches a fresh stream
                // before POST. Its watch readiness can wake the POST task
                // before this Connected control message reaches the UI loop;
                // a pending GET issued here could then observe the consumed
                // question as `none` and invalidate the still-pending
                // AnswerSubmitted result. The answer outcome owns
                // reconciliation while the modal is submitting.
                if self
                    .pending_question
                    .as_ref()
                    .is_none_or(|question| !question.submitting)
                {
                    self.reconcile_pending_question_after_stream_connect(session_id);
                }
                Ok(())
            }
            SessionSseEvent::TransportFailed {
                session_id,
                stream_epoch,
                message,
            } => {
                if self.chat.session_id.as_deref() != Some(session_id.as_str())
                    || self.sse_epoch != stream_epoch
                {
                    return Ok(());
                }
                // Transport failure is not an AgentEvent::Error: the server
                // may still be waiting on the visible question. Detach the
                // exhausted stream but preserve modal/dismissed state, draft,
                // identity, and answer epoch for a safe retry.
                self.detach_stream();
                self.connected = false;
                self.stream_disconnected = true;
                self.notify(NoticeLevel::Error, format!("SSE disconnected: {message}"));
                Ok(())
            }
        }
    }

    fn handle_sse_event(&mut self, event: AgentEvent) -> Result<()> {
        if self.chat.auto_scroll {
            // Any incoming event means new content; auto_scroll will reposition.
        }
        match event {
            AgentEvent::Token { content } => {
                self.chat.current_response.push_str(&content);
                self.chat.auto_scroll = true;
            }
            AgentEvent::ExecutionStarted { .. } => {
                if self
                    .pending_question
                    .as_ref()
                    .is_some_and(|question| question.submitting)
                {
                    self.pending_answer_run_started = true;
                }
                self.stream_disconnected = false;
            }
            AgentEvent::ReasoningToken { content } => {
                self.chat.current_reasoning.push_str(&content);
            }
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                self.chat.current_tool_calls.push(ToolCallDisplay {
                    id: tool_call_id,
                    tool_name,
                    arguments: serde_json::to_string(&arguments).unwrap_or_default(),
                    result: None,
                    error: None,
                    phase: "running".to_string(),
                });
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => match self.find_tool_mut(&tool_call_id) {
                Some(tc) => {
                    tc.result = Some(result.result);
                    tc.phase = "complete".to_string();
                }
                None => {
                    // No matching ToolStart (dropped/out-of-order) — surface it
                    // defensively instead of silently losing the result.
                    self.chat.current_tool_calls.push(ToolCallDisplay {
                        id: tool_call_id,
                        tool_name: "unknown".to_string(),
                        arguments: String::new(),
                        result: Some(result.result),
                        error: None,
                        phase: "complete".to_string(),
                    });
                }
            },
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => match self.find_tool_mut(&tool_call_id) {
                Some(tc) => {
                    tc.error = Some(error);
                    tc.phase = "error".to_string();
                }
                None => {
                    self.chat.current_tool_calls.push(ToolCallDisplay {
                        id: tool_call_id,
                        tool_name: "unknown".to_string(),
                        arguments: String::new(),
                        result: None,
                        error: Some(error),
                        phase: "error".to_string(),
                    });
                }
            },
            AgentEvent::ToolLifecycle {
                tool_call_id,
                phase,
                summary,
                error,
                ..
            } => {
                // Lifecycle phases ("begin"/"executing"/"finished"/"cancelled")
                // are supplementary progress strings, not the UI's terminal
                // vocabulary ("complete"/"error") — once ToolComplete/ToolError
                // has set one of those, a later Lifecycle event must not
                // overwrite it back to a non-terminal phase (the UI's ✓/✗ icon
                // is keyed on those exact strings).
                if let Some(tc) = self.find_tool_mut(&tool_call_id) {
                    if tc.phase != "complete" && tc.phase != "error" {
                        tc.phase = phase;
                        if let Some(s) = summary {
                            tc.result = Some(s);
                        }
                        if let Some(e) = error {
                            tc.error = Some(e);
                        }
                    }
                }
                // No matching entry: a Lifecycle event with no known Start is
                // dropped (it carries only supplementary progress info, unlike
                // Complete/Error's definitive terminal result).
            }
            AgentEvent::NeedClarification {
                question,
                options,
                tool_call_id,
                tool_name,
                allow_custom,
                source,
            } => {
                let Some(session_id) = self.chat.session_id.clone() else {
                    self.notify(
                        NoticeLevel::Warn,
                        "Ignored clarification without an active session",
                    );
                    return Ok(());
                };
                let pending = PendingQuestion {
                    has_pending_question: true,
                    question,
                    options,
                    allow_custom,
                    tool_call_id,
                    tool_name,
                    source,
                };
                let incoming = self.question_from_pending(session_id, &pending);
                let needs_identity_sync = incoming.tool_call_id.is_none();
                if let Some(existing) = self.pending_question.as_mut() {
                    if existing.identity() == incoming.identity() {
                        // Critical-event replay/reconnect may deliver the same
                        // question again. Refresh its contract without resetting
                        // selection, draft, inspector, error, or in-flight state.
                        existing.refresh_contract(incoming);
                        if needs_identity_sync {
                            self.status_message = "Synchronizing question identity...".to_string();
                            if let Some(session_id) = self.chat.session_id.clone() {
                                self.reconcile_pending_question_after_stream_connect(session_id);
                            }
                        }
                        return Ok(());
                    }
                }
                if self.pending_question.is_none() {
                    if let Some(dismissed) = self.dismissed_question.as_mut() {
                        if dismissed.identity() == incoming.identity() {
                            // Replayed critical events must not undo an explicit
                            // Esc dismissal. Refresh the typed contract/draft in
                            // place and keep the question cached for Ctrl+Q.
                            dismissed.refresh_contract(incoming);
                            self.status_message =
                                "Question remains dismissed (Ctrl+Q to reopen)".to_string();
                            if needs_identity_sync {
                                if let Some(session_id) = self.chat.session_id.clone() {
                                    self.reconcile_pending_question_after_stream_connect(
                                        session_id,
                                    );
                                }
                            }
                            return Ok(());
                        }
                    }
                }
                // A new question supersedes any answer still in flight for a
                // previous one — a late response must not clear this modal.
                self.stash_question_drafts();
                self.supersede_pending_answer();
                self.pending_answer_run_started = false;
                self.dismissed_question = None;
                self.status_message =
                    format!("Question: {} (answer in the dialog)", incoming.question);
                self.pending_question = Some(incoming);
                if needs_identity_sync {
                    self.status_message = "Synchronizing question identity...".to_string();
                    if let Some(session_id) = self.chat.session_id.clone() {
                        self.reconcile_pending_question_after_stream_connect(session_id);
                    }
                }
            }
            AgentEvent::Complete { usage } => self.handle_complete(usage),
            AgentEvent::Cancelled { message } => {
                self.status_message = message.unwrap_or_else(|| "Cancelled".to_string());
                self.finalize_streaming();
            }
            AgentEvent::Error { message } => {
                self.notify(NoticeLevel::Error, format!("Error: {message}"));
                self.finalize_streaming();
            }
            AgentEvent::BudgetExceeded {
                kind,
                limit,
                actual,
            } => {
                // Precedes the run's normal `Complete`/`Cancelled` terminal
                // event (see bamboo_agent_core::AgentEvent::BudgetExceeded) —
                // just surface why the run is about to stop; the terminal
                // event still finalizes streaming.
                self.notify(
                    NoticeLevel::Warn,
                    format!("Run budget exceeded ({kind}: {actual}/{limit}) — stopping."),
                );
            }
            AgentEvent::ToolToken { content, .. } => {
                // Deliberately not routed into the matching ToolCallDisplay by
                // `tool_call_id`: today's rendering already prints tool output
                // inline with the response text, and threading a per-call
                // streaming buffer through to the UI is beyond this fix's
                // scope. Kept as the simpler, behavior-preserving option.
                self.chat.current_response.push_str(&content);
            }
            AgentEvent::ContextCompressionStatus { phase, status } => {
                self.status_message = format!("Compression: {} ({})", status, phase);
            }
            AgentEvent::PlanModeEntered { reason, .. } => {
                self.chat.plan_mode = true;
                self.status_message = format!(
                    "Plan mode active{}",
                    reason
                        .as_ref()
                        .map(|r| format!(": {}", r))
                        .unwrap_or_default()
                );
            }
            AgentEvent::PlanModeExited { approved, .. } => {
                self.chat.plan_mode = false;
                self.status_message = if approved {
                    "Plan mode exited (approved)".to_string()
                } else {
                    "Plan mode exited".to_string()
                };
            }
            AgentEvent::PlanFileUpdated { file_path, .. } => {
                self.status_message = format!("Plan updated: {}", file_path);
            }
            AgentEvent::SubAgentStarted {
                child_session_id,
                title,
            } => {
                if !self
                    .chat
                    .sub_agents
                    .iter()
                    .any(|s| s.child_session_id == child_session_id)
                {
                    self.chat.sub_agents.push(SubAgentDisplay {
                        child_session_id,
                        title,
                        status: "running".to_string(),
                    });
                }
            }
            AgentEvent::SubAgentHeartbeat { .. } => {}
            AgentEvent::SubAgentCompleted {
                child_session_id,
                status,
                ..
            } => {
                if let Some(sa) = self
                    .chat
                    .sub_agents
                    .iter_mut()
                    .find(|s| s.child_session_id == child_session_id)
                {
                    sa.status = status;
                }
            }
        }
        Ok(())
    }

    fn finalize_streaming(&mut self) {
        self.chat.streaming = false;
        self.flush_streaming_output();
        self.status_message = "Ready".to_string();
        self.detach_stream();
        self.stream_disconnected = false;
        self.pending_answer_run_started = false;
        self.chat.sub_agents.clear();
        // A run that ended (completed / cancelled / stopped) can no longer accept
        // an answer, so drop any open (or dismissed-but-cached) question modal
        // to avoid answering a dead session — and invalidate any answer POST
        // still in flight for it.
        self.supersede_pending_answer();
        self.pending_question = None;
        self.dismissed_question = None;
    }

    fn handle_complete(&mut self, usage: TokenUsage) {
        let answer_in_flight = self
            .pending_question
            .as_ref()
            .is_some_and(|question| question.submitting);
        if (self.pending_question.is_some() || self.dismissed_question.is_some())
            && !self.pending_answer_run_started
            && !answer_in_flight
        {
            // Legacy servers may still expose the Complete emitted by the
            // activation that suspended at NeedClarification. That boundary is
            // not terminal user work: preserve the exact modal/draft and keep
            // the session input-blocked.
            self.flush_streaming_output();
            self.chat.token_usage = Some(usage);
            self.chat.streaming = true;
            self.chat.sub_agents.clear();
            self.status_message = "Paused — waiting for your answer".to_string();
            if let Some(session_id) = self.chat.session_id.clone() {
                self.attach_stream(session_id);
            }
            return;
        }
        self.finalize_streaming();
        self.chat.token_usage = Some(usage);
    }

    fn flush_streaming_output(&mut self) {
        if !self.chat.current_response.is_empty() || !self.chat.current_tool_calls.is_empty() {
            self.chat.messages.push(ChatMessage {
                role: MessageRole::Assistant,
                content: std::mem::take(&mut self.chat.current_response),
                tool_calls: std::mem::take(&mut self.chat.current_tool_calls),
                reasoning: if self.chat.current_reasoning.is_empty() {
                    None
                } else {
                    Some(std::mem::take(&mut self.chat.current_reasoning))
                },
            });
        }
    }

    /// Stop the current run WITHOUT blocking the event loop: the `stop` POST
    /// is spawned off the UI thread and its outcome comes back as
    /// `AppEvent::StopFinished` (handled in `handle_event`, which finalizes
    /// streaming either way). Previously this awaited `client.stop()` and
    /// `?`-propagated a network error — a dead server hit at the worst
    /// possible moment (pressing Ctrl+C to stop a run) tore down the whole
    /// TUI instead of just failing the stop.
    fn stop_streaming(&mut self) {
        // Cancel a clarification answer before launching the stop request.
        // In particular this prevents a task blocked on SSE readiness from
        // waking later and POSTing into a session the operator already
        // stopped.
        self.supersede_pending_answer();
        let Some(sid) = self.chat.session_id.clone() else {
            // Nothing to stop server-side; still clear local streaming state.
            self.finalize_streaming();
            self.status_message = "Stopped".to_string();
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            self.finalize_streaming();
            return;
        };
        self.status_message = "Stopping...".to_string();
        let client = self.client.clone();
        tokio::spawn(async move {
            let r = client.stop(&sid).await.map_err(|e| e.to_string());
            let _ = tx.send(AppEvent::StopFinished(r));
        });
    }

    async fn handle_sessions_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down if !self.sessions.sessions.is_empty() => {
                self.sessions.selected =
                    (self.sessions.selected + 1).min(self.sessions.sessions.len() - 1);
            }
            KeyCode::Up => {
                self.sessions.selected = self.sessions.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                // Full resume, off the event loop: history replay + live
                // reattach (if the run is still going) + pending-question
                // recovery, all landing in one `SessionOpened` event.
                if let Some(session) = self.sessions.sessions.get(self.sessions.selected) {
                    self.resume_session(session.id.clone());
                }
            }
            KeyCode::Char('d') => {
                // Open a confirmation modal instead of deleting immediately — the
                // actual DELETE is fired from `handle_delete_confirm_key` off the
                // event loop, never `?`-propagated on the UI thread.
                if let Some(session) = self.sessions.sessions.get(self.sessions.selected) {
                    self.pending_delete = Some((session.id.clone(), session.title.clone()));
                }
            }
            KeyCode::Char('r') => {
                self.load_tab_data();
            }
            KeyCode::Char(']') => {
                if let Some(next) = self.sessions.next_offset {
                    self.sessions.offset = next;
                    self.sessions.selected = 0;
                    self.load_tab_data();
                }
            }
            KeyCode::Char('[') if self.sessions.offset > 0 => {
                let step = self.sessions.page_limit.max(1);
                self.sessions.offset = self.sessions.offset.saturating_sub(step);
                self.sessions.selected = 0;
                self.load_tab_data();
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_mcp_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down if !self.mcp.servers.is_empty() => {
                self.mcp.selected = (self.mcp.selected + 1).min(self.mcp.servers.len() - 1);
            }
            KeyCode::Up => {
                self.mcp.selected = self.mcp.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                if let (Some(server), Some(tx)) = (
                    self.mcp.servers.get(self.mcp.selected),
                    self.event_tx.clone(),
                ) {
                    let id = server.id.clone();
                    let connected = server.connected.unwrap_or(false);
                    let client = self.client.clone();
                    self.mcp.loading = true;
                    tokio::spawn(async move {
                        let res = if connected {
                            client.disconnect_mcp(&id).await
                        } else {
                            client.connect_mcp(&id).await
                        };
                        let outcome = match res {
                            Ok(()) => Ok(if connected {
                                "Disconnected".to_string()
                            } else {
                                "Connected".to_string()
                            }),
                            Err(e) => Err(format!("MCP action failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                            session_picker_epoch: None,
                        });
                    });
                }
            }
            KeyCode::Char('t') => {
                if let (Some(server), Some(tx)) = (
                    self.mcp.servers.get(self.mcp.selected),
                    self.event_tx.clone(),
                ) {
                    let id = server.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let r = client.get_mcp_tools(&id).await.map_err(|e| e.to_string());
                        let _ = tx.send(AppEvent::McpToolsLoaded(r));
                    });
                }
            }
            KeyCode::Char('r') => {
                self.load_tab_data();
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_schedules_key(&mut self, key: KeyEvent) -> Result<()> {
        // Note: when `schedule_form` is open, `handle_key` routes every key
        // straight to `handle_schedule_form_key` before reaching here.
        match key.code {
            KeyCode::Char('n') => {
                self.schedule_form = Some(ScheduleForm::default());
            }
            KeyCode::Down if !self.schedules.schedules.is_empty() => {
                self.schedules.selected =
                    (self.schedules.selected + 1).min(self.schedules.schedules.len() - 1);
            }
            KeyCode::Up => {
                self.schedules.selected = self.schedules.selected.saturating_sub(1);
            }
            KeyCode::Char('d') => {
                // Spawned off the event loop like every other mutation here
                // (see `handle_mcp_key`'s connect/disconnect) — an inline
                // `.await` on the UI thread would freeze the whole app
                // (spinner, redraw, SSE receipt) for the round-trip.
                if let (Some(schedule), Some(tx)) = (
                    self.schedules.schedules.get(self.schedules.selected),
                    self.event_tx.clone(),
                ) {
                    let id = schedule.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.delete_schedule(&id).await {
                            Ok(()) => Ok("Schedule deleted".to_string()),
                            Err(e) => Err(format!("Delete failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                            session_picker_epoch: None,
                        });
                    });
                }
            }
            KeyCode::Char('r') => {
                if let (Some(schedule), Some(tx)) = (
                    self.schedules.schedules.get(self.schedules.selected),
                    self.event_tx.clone(),
                ) {
                    let id = schedule.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.run_schedule_now(&id).await {
                            Ok(()) => Ok("Schedule triggered".to_string()),
                            Err(e) => Err(format!("Run failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: false,
                            session_picker_epoch: None,
                        });
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Drive the new-schedule form modal. On Enter (all fields filled) it POSTs
    /// create_schedule off the event loop and reloads the tab.
    fn handle_schedule_form_key(&mut self, key: KeyEvent) {
        let Some(form) = self.schedule_form.as_mut() else {
            return;
        };
        match key.code {
            KeyCode::Esc => {
                self.schedule_form = None;
            }
            KeyCode::Tab | KeyCode::Down => {
                form.field = (form.field + 1) % 3;
            }
            KeyCode::BackTab | KeyCode::Up => {
                form.field = (form.field + 2) % 3;
            }
            KeyCode::Backspace => {
                form.current_mut().pop();
            }
            KeyCode::Enter => {
                if form.name.trim().is_empty()
                    || form.cron.trim().is_empty()
                    || form.prompt.trim().is_empty()
                {
                    self.status_message = "Fill in name, cron, and prompt".to_string();
                    return;
                }
                let req = CreateScheduleRequest {
                    name: form.name.trim().to_string(),
                    trigger: ScheduleTriggerReq::Cron {
                        expr: form.cron.trim().to_string(),
                    },
                    enabled: true,
                    run_config: ScheduleRunConfigReq {
                        project_id: self
                            .sessions
                            .sessions
                            .get(self.sessions.selected)
                            .and_then(|session| session.project_id.clone()),
                        task_message: Some(form.prompt.trim().to_string()),
                        auto_execute: true,
                    },
                };
                self.schedule_form = None;
                if let Some(tx) = self.event_tx.clone() {
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let outcome = match client.create_schedule(req).await {
                            Ok(()) => Ok("Schedule created".to_string()),
                            Err(e) => Err(format!("Create failed: {e}")),
                        };
                        let _ = tx.send(AppEvent::ActionDone {
                            outcome,
                            reload_tab: true,
                            session_picker_epoch: None,
                        });
                    });
                }
            }
            KeyCode::Char(c) => {
                form.current_mut().push(c);
            }
            _ => {}
        }
    }

    async fn handle_skills_key(&mut self, key: KeyEvent) -> Result<()> {
        match key.code {
            KeyCode::Down if !self.skills.skills.is_empty() => {
                self.skills.selected = (self.skills.selected + 1).min(self.skills.skills.len() - 1);
            }
            KeyCode::Up => {
                self.skills.selected = self.skills.selected.saturating_sub(1);
            }
            KeyCode::Enter => {
                // Fetch the detail off the event loop: `get_skill` used to be
                // awaited right here on the UI thread, so a slow/unreachable
                // server froze every keystroke until it returned.
                if let (Some(skill), Some(tx)) = (
                    self.skills.skills.get(self.skills.selected),
                    self.event_tx.clone(),
                ) {
                    let id = skill.id.clone();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let r = client.get_skill(&id).await.map_err(|e| e.to_string());
                        let _ = tx.send(AppEvent::SkillDetailLoaded(r));
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Record a notification in the scrollback log and mirror it to the
    /// transient status line. Warn/error entries bump the unseen-alert badge
    /// until the log is opened. The log is capped so it can't grow unbounded.
    fn notify(&mut self, level: NoticeLevel, text: impl Into<String>) {
        let text = text.into();
        if matches!(level, NoticeLevel::Warn | NoticeLevel::Error) {
            self.unseen_alerts = self.unseen_alerts.saturating_add(1);
        }
        self.status_message = text.clone();
        self.notifications.push(Notification {
            level,
            text,
            at: Utc::now(),
        });
        const CAP: usize = 200;
        if self.notifications.len() > CAP {
            let excess = self.notifications.len() - CAP;
            self.notifications.drain(0..excess);
        }
    }

    fn handle_config_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => self.config_scroll_down(1),
            KeyCode::Up => self.config_scroll_up(1),
            KeyCode::PageDown => self.config_scroll_down(10),
            KeyCode::PageUp => self.config_scroll_up(10),
            KeyCode::Char('e') => {
                // Open the raw-JSON editor prefilled with the current config.
                match &self.config.config {
                    Some(val) => {
                        let pretty =
                            serde_json::to_string_pretty(val).unwrap_or_else(|_| val.to_string());
                        let lines: Vec<String> = pretty.lines().map(String::from).collect();
                        self.config_editor = Some(ConfigEditor {
                            textarea: TextArea::new(lines),
                        });
                    }
                    None => {
                        self.notify(NoticeLevel::Warn, "No config loaded to edit");
                    }
                }
            }
            _ => {}
        }
    }

    /// Drive the raw-JSON config editor modal. `Ctrl+S` validates the buffer as
    /// JSON and, if valid, PATCHes it to the server off the event loop; `Esc`
    /// cancels. Every other key edits the text buffer.
    fn handle_config_editor_key(&mut self, key: KeyEvent) -> Result<()> {
        if key.code == KeyCode::Esc {
            self.config_editor = None;
            self.status_message = "Edit cancelled".to_string();
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s') {
            let text = self
                .config_editor
                .as_ref()
                .map(|e| e.textarea.lines().join("\n"))
                .unwrap_or_default();
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(val) => {
                    self.config_editor = None;
                    self.status_message = "Saving config...".to_string();
                    if let Some(tx) = self.event_tx.clone() {
                        let client = self.client.clone();
                        tokio::spawn(async move {
                            let outcome = match client.set_config(&val).await {
                                Ok(()) => Ok("Config saved".to_string()),
                                Err(e) => Err(format!("Save failed: {e}")),
                            };
                            let _ = tx.send(AppEvent::ActionDone {
                                outcome,
                                reload_tab: true,
                                session_picker_epoch: None,
                            });
                        });
                    }
                }
                Err(e) => {
                    self.notify(NoticeLevel::Error, format!("Invalid JSON: {e}"));
                }
            }
            return Ok(());
        }
        if let Some(editor) = self.config_editor.as_mut() {
            editor.textarea.input(key);
        }
        Ok(())
    }

    // ── Contextual session picker (Ctrl+P) ──

    fn open_session_picker(&mut self) {
        self.picker_epoch = self.picker_epoch.wrapping_add(1);
        let epoch = self.picker_epoch;
        self.session_picker = Some(SessionPicker {
            epoch,
            sessions: Vec::new(),
            visible: Vec::new(),
            query: String::new(),
            selected: 0,
            selection_touched: false,
            loading: true,
            error: None,
            total: 0,
            page_limit: 0,
            next_offset: None,
            mode: SessionPickerMode::Browse,
        });
        self.load_session_picker_page(epoch, 0);
    }

    fn close_session_picker(&mut self) {
        if let Some(task) = self.session_picker_task.take() {
            task.abort();
        }
        self.session_picker = None;
    }

    /// Give every page or mutation request its own generation. Aborting a
    /// Tokio task is best-effort: its event may already be queued, so changing
    /// the epoch is what prevents an old response from satisfying a later
    /// rename or pin operation for the same session.
    fn advance_session_picker_epoch(&mut self) -> Option<u64> {
        if let Some(task) = self.session_picker_task.take() {
            task.abort();
        }
        self.picker_epoch = self.picker_epoch.wrapping_add(1);
        let picker = self.session_picker.as_mut()?;
        picker.epoch = self.picker_epoch;
        Some(self.picker_epoch)
    }

    fn reload_session_picker(&mut self) {
        let Some(picker) = self.session_picker.as_mut() else {
            return;
        };
        if let Some(task) = self.session_picker_task.take() {
            task.abort();
        }
        self.picker_epoch = self.picker_epoch.wrapping_add(1);
        picker.epoch = self.picker_epoch;
        picker.sessions.clear();
        picker.visible.clear();
        picker.selected = 0;
        picker.selection_touched = false;
        picker.loading = true;
        picker.error = None;
        picker.total = 0;
        picker.page_limit = 0;
        picker.next_offset = None;
        picker.mode = SessionPickerMode::Browse;
        self.load_session_picker_page(self.picker_epoch, 0);
    }

    fn load_session_picker_page(&mut self, epoch: u64, offset: usize) {
        let Some(tx) = self.event_tx.clone() else {
            if let Some(picker) = self.session_picker.as_mut() {
                picker.loading = false;
                picker.error = Some("Session picker is not attached to an event loop".to_string());
            }
            return;
        };
        let Some(picker) = self
            .session_picker
            .as_mut()
            .filter(|picker| picker.epoch == epoch)
        else {
            return;
        };
        picker.loading = true;
        let limit = (picker.page_limit > 0).then_some(picker.page_limit);
        let client = self.client.clone();
        self.session_picker_task = Some(tokio::spawn(async move {
            let result = client
                .list_sessions(limit, (offset > 0).then_some(offset))
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(AppEvent::SessionPickerPageLoaded {
                epoch,
                offset,
                result,
            });
        }));
    }

    fn load_next_session_picker_page(&mut self) {
        if self.session_picker_task.is_some() {
            return;
        }
        let Some((epoch, offset)) = self
            .session_picker
            .as_ref()
            .and_then(|picker| picker.next_offset.map(|offset| (picker.epoch, offset)))
        else {
            return;
        };
        self.load_session_picker_page(epoch, offset);
    }

    async fn handle_session_picker_key(&mut self, key: KeyEvent) -> Result<()> {
        let mode = self
            .session_picker
            .as_ref()
            .map(|picker| match picker.mode {
                SessionPickerMode::Browse => 0,
                SessionPickerMode::Rename { .. } => 1,
                SessionPickerMode::Pinning { .. } => 2,
            });
        match mode {
            Some(0) => self.handle_session_picker_browse_key(key),
            Some(1) => self.handle_session_picker_rename_key(key),
            Some(2) => self.handle_session_picker_pinning_key(key),
            _ => {}
        }
        Ok(())
    }

    fn handle_session_picker_browse_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('r') => {
                    self.reload_session_picker();
                    return;
                }
                KeyCode::Char('u') => {
                    if let Some(picker) = self.session_picker.as_mut() {
                        let preserve = picker.selected_session().map(|s| s.id.clone());
                        picker.query.clear();
                        picker.selection_touched = true;
                        picker.refresh_filter(preserve);
                    }
                    return;
                }
                KeyCode::Char('d') => {
                    if let Some(session) = self
                        .session_picker
                        .as_ref()
                        .and_then(SessionPicker::selected_session)
                    {
                        self.pending_delete = Some((session.id.clone(), session.title.clone()));
                    }
                    return;
                }
                _ => {}
            }
        }

        match key.code {
            KeyCode::Up => {
                if let Some(picker) = self.session_picker.as_mut() {
                    picker.selected = picker.selected.saturating_sub(1);
                    picker.selection_touched = true;
                }
            }
            KeyCode::Down => {
                let load_more = if let Some(picker) = self.session_picker.as_mut() {
                    picker.selection_touched = true;
                    if picker.selected + 1 < picker.visible.len() {
                        picker.selected += 1;
                        false
                    } else {
                        picker.next_offset.is_some()
                    }
                } else {
                    false
                };
                if load_more {
                    self.load_next_session_picker_page();
                }
            }
            KeyCode::PageDown | KeyCode::Char(']') => self.load_next_session_picker_page(),
            KeyCode::Enter => {
                let session_id = self
                    .session_picker
                    .as_ref()
                    .and_then(SessionPicker::selected_session)
                    .map(|session| session.id.clone());
                if let Some(session_id) = session_id {
                    self.close_session_picker();
                    self.resume_session(session_id);
                }
            }
            KeyCode::F(2) => self.begin_session_rename(),
            KeyCode::F(3) => self.begin_session_pin_toggle(),
            KeyCode::Delete => {
                if let Some(session) = self
                    .session_picker
                    .as_ref()
                    .and_then(SessionPicker::selected_session)
                {
                    self.pending_delete = Some((session.id.clone(), session.title.clone()));
                }
            }
            KeyCode::Esc => self.close_session_picker(),
            KeyCode::Backspace => {
                if let Some(picker) = self.session_picker.as_mut() {
                    let preserve = picker.selected_session().map(|s| s.id.clone());
                    picker.query.pop();
                    picker.selection_touched = true;
                    picker.refresh_filter(preserve);
                }
            }
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                if let Some(picker) = self.session_picker.as_mut() {
                    let preserve = picker.selected_session().map(|s| s.id.clone());
                    picker.query.push(character);
                    picker.selection_touched = true;
                    picker.refresh_filter(preserve);
                }
                if self
                    .session_picker
                    .as_ref()
                    .is_some_and(|picker| !picker.query.is_empty() && picker.next_offset.is_some())
                {
                    self.load_next_session_picker_page();
                }
            }
            _ => {}
        }
    }

    fn begin_session_rename(&mut self) {
        let Some((session_id, draft)) = self.session_picker.as_ref().and_then(|picker| {
            picker
                .selected_session()
                .map(|session| (session.id.clone(), session.title.clone()))
        }) else {
            return;
        };
        let Some(epoch) = self.advance_session_picker_epoch() else {
            return;
        };
        if let Some(picker) = self.session_picker.as_mut() {
            picker.mode = SessionPickerMode::Rename {
                session_id: session_id.clone(),
                base_title: draft.clone(),
                draft,
                draft_dirty: false,
                metadata_version: None,
                loading_version: true,
                submitting: false,
                error: None,
            };
        }
        self.spawn_session_picker_version_load(epoch, session_id, SessionPickerIntent::Rename);
    }

    fn begin_session_pin_toggle(&mut self) {
        let Some((session_id, target)) = self.session_picker.as_ref().and_then(|picker| {
            picker
                .selected_session()
                .map(|session| (session.id.clone(), !session.pinned))
        }) else {
            return;
        };
        let Some(epoch) = self.advance_session_picker_epoch() else {
            return;
        };
        if let Some(picker) = self.session_picker.as_mut() {
            picker.mode = SessionPickerMode::Pinning {
                session_id: session_id.clone(),
                target,
                loading_version: true,
                submitting: false,
                error: None,
            };
        }
        self.spawn_session_picker_version_load(epoch, session_id, SessionPickerIntent::Pin(target));
    }

    fn handle_session_picker_rename_key(&mut self, key: KeyEvent) {
        let mut retry = None;
        let mut submit = None;
        let mut cancel = false;
        let Some(picker) = self.session_picker.as_mut() else {
            return;
        };
        let SessionPickerMode::Rename {
            session_id,
            draft,
            draft_dirty,
            metadata_version,
            loading_version,
            submitting,
            error,
            ..
        } = &mut picker.mode
        else {
            return;
        };

        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
            if !*submitting {
                *metadata_version = None;
                *loading_version = true;
                *error = None;
                retry = Some((picker.epoch, session_id.clone()));
            }
        } else if *submitting {
            return;
        } else {
            match key.code {
                KeyCode::Enter if !draft.trim().is_empty() => {
                    if let Some(version) = *metadata_version {
                        *submitting = true;
                        *error = None;
                        submit = Some((
                            picker.epoch,
                            session_id.clone(),
                            version,
                            draft.trim().to_string(),
                        ));
                    } else if !*loading_version {
                        *error = Some("No current version — press Ctrl+R to retry".to_string());
                    }
                }
                KeyCode::Esc => cancel = true,
                KeyCode::Backspace => {
                    draft.pop();
                    *draft_dirty = true;
                    *error = None;
                }
                KeyCode::Char(character)
                    if !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    draft.push(character);
                    *draft_dirty = true;
                    *error = None;
                }
                _ => {}
            }
        }

        if cancel {
            self.advance_session_picker_epoch();
            if let Some(picker) = self.session_picker.as_mut() {
                picker.mode = SessionPickerMode::Browse;
            }
        } else if let Some((epoch, session_id)) = retry {
            let epoch = self.advance_session_picker_epoch().unwrap_or(epoch);
            self.spawn_session_picker_version_load(epoch, session_id, SessionPickerIntent::Rename);
        } else if let Some((epoch, session_id, version, title)) = submit {
            self.spawn_session_picker_patch(
                epoch,
                session_id,
                version,
                SessionPickerIntent::Rename,
                PatchSessionMetadataRequest {
                    title: Some(title),
                    pinned: None,
                },
            );
        }
    }

    fn handle_session_picker_pinning_key(&mut self, key: KeyEvent) {
        let mut retry = None;
        let mut cancel = false;
        let Some(picker) = self.session_picker.as_mut() else {
            return;
        };
        let SessionPickerMode::Pinning {
            session_id,
            target,
            loading_version,
            submitting,
            error,
        } = &mut picker.mode
        else {
            return;
        };
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
            if !*submitting {
                *loading_version = true;
                *error = None;
                retry = Some((picker.epoch, session_id.clone(), *target));
            }
        } else if key.code == KeyCode::Esc && !*submitting {
            cancel = true;
        }

        if cancel {
            self.advance_session_picker_epoch();
            if let Some(picker) = self.session_picker.as_mut() {
                picker.mode = SessionPickerMode::Browse;
            }
        } else if let Some((epoch, session_id, target)) = retry {
            let epoch = self.advance_session_picker_epoch().unwrap_or(epoch);
            self.spawn_session_picker_version_load(
                epoch,
                session_id,
                SessionPickerIntent::Pin(target),
            );
        }
    }

    fn spawn_session_picker_version_load(
        &mut self,
        epoch: u64,
        session_id: String,
        intent: SessionPickerIntent,
    ) {
        if let Some(task) = self.session_picker_task.take() {
            task.abort();
        }
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        self.session_picker_task = Some(tokio::spawn(async move {
            let result = client
                .get_session_versioned(&session_id)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(AppEvent::SessionPickerVersionLoaded {
                epoch,
                session_id,
                intent,
                result,
            });
        }));
    }

    fn spawn_session_picker_patch(
        &mut self,
        epoch: u64,
        session_id: String,
        expected_version: u64,
        intent: SessionPickerIntent,
        patch: PatchSessionMetadataRequest,
    ) {
        if let Some(task) = self.session_picker_task.take() {
            task.abort();
        }
        let Some(tx) = self.event_tx.clone() else {
            return;
        };
        let client = self.client.clone();
        self.session_picker_task = Some(tokio::spawn(async move {
            let result = client
                .patch_session_metadata(&session_id, expected_version, &patch)
                .await;
            let _ = tx.send(AppEvent::SessionPickerPatched {
                epoch,
                session_id,
                intent,
                result,
            });
        }));
    }

    // ── Searchable model picker (Ctrl+O) ──

    fn open_model_picker(&mut self) {
        self.picker_epoch = self.picker_epoch.wrapping_add(1);
        let epoch = self.picker_epoch;
        self.model_picker = Some(ModelPicker {
            epoch,
            models: Vec::new(),
            visible: Vec::new(),
            query: String::new(),
            selected: 0,
            loading: true,
            applying: false,
            error: None,
        });
        self.load_model_catalog(epoch);
    }

    fn reload_model_catalog(&mut self) {
        if let Some(task) = self.model_picker_task.take() {
            task.abort();
        }
        self.picker_epoch = self.picker_epoch.wrapping_add(1);
        let Some(picker) = self.model_picker.as_mut() else {
            return;
        };
        picker.epoch = self.picker_epoch;
        self.load_model_catalog(self.picker_epoch);
    }

    fn load_model_catalog(&mut self, epoch: u64) {
        if let Some(task) = self.model_picker_task.take() {
            task.abort();
        }
        let Some(tx) = self.event_tx.clone() else {
            if let Some(picker) = self.model_picker.as_mut() {
                picker.loading = false;
                picker.error = Some("Model picker is not attached to an event loop".to_string());
            }
            return;
        };
        if let Some(picker) = self.model_picker.as_mut() {
            picker.loading = true;
            picker.error = None;
        }
        let client = self.client.clone();
        self.model_picker_task = Some(tokio::spawn(async move {
            let result = client
                .get_provider_catalog()
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(AppEvent::CatalogLoaded { epoch, result });
        }));
    }

    fn close_model_picker(&mut self) {
        if let Some(task) = self.model_picker_task.take() {
            task.abort();
        }
        self.model_picker = None;
    }

    async fn handle_model_picker_key(&mut self, key: KeyEvent) -> Result<()> {
        let Some(picker) = self.model_picker.as_ref() else {
            return Ok(());
        };
        if picker.applying {
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('u') {
            if let Some(picker) = self.model_picker.as_mut() {
                let preserve = picker.selected_model().map(model_key);
                picker.query.clear();
                picker.refresh_filter(preserve);
            }
            return Ok(());
        }

        match key.code {
            KeyCode::Up => {
                if let Some(picker) = self.model_picker.as_mut() {
                    picker.selected = picker.selected.saturating_sub(1);
                }
            }
            KeyCode::Down => {
                if let Some(picker) = self.model_picker.as_mut() {
                    if picker.selected + 1 < picker.visible.len() {
                        picker.selected += 1;
                    }
                }
            }
            KeyCode::Enter => {
                let model = self
                    .model_picker
                    .as_ref()
                    .and_then(ModelPicker::selected_model)
                    .cloned();
                if let Some(model) = model {
                    self.apply_model(model);
                }
            }
            KeyCode::Esc => self.close_model_picker(),
            KeyCode::Backspace => {
                if let Some(picker) = self.model_picker.as_mut() {
                    let preserve = picker.selected_model().map(model_key);
                    picker.query.pop();
                    picker.refresh_filter(preserve);
                }
            }
            KeyCode::Char('r')
                if key.modifiers.contains(KeyModifiers::CONTROL)
                    && self
                        .model_picker
                        .as_ref()
                        .is_some_and(|picker| !picker.loading && picker.visible.is_empty()) =>
            {
                self.reload_model_catalog();
            }
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                if let Some(picker) = self.model_picker.as_mut() {
                    let preserve = picker.selected_model().map(model_key);
                    picker.query.push(character);
                    picker.refresh_filter(preserve);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Apply only after the server accepts the active-session PATCH. This
    /// avoids the old half-applied state where the local badge changed and the
    /// modal closed even though persistence failed.
    fn apply_model(&mut self, model: CatalogModel) {
        let Some(session_id) = self.chat.session_id.clone() else {
            self.commit_model_selection(model);
            return;
        };
        let Some(tx) = self.event_tx.clone() else {
            if let Some(picker) = self.model_picker.as_mut() {
                picker.error = Some("Model update cannot run without the event loop".to_string());
            }
            return;
        };
        let Some(picker) = self.model_picker.as_mut() else {
            return;
        };
        picker.applying = true;
        picker.error = None;
        let epoch = picker.epoch;
        let model_ref = model.reference.clone();
        let client = self.client.clone();
        self.model_picker_task = Some(tokio::spawn(async move {
            let result = client
                .patch_session_model(&session_id, &model_ref)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(AppEvent::ModelPatched {
                epoch,
                session_id,
                model,
                result,
            });
        }));
    }

    pub(crate) fn model_group_label(&self, model: &CatalogModel) -> String {
        let current = self.model_picker.as_ref().and_then(|picker| {
            current_model_key(
                &picker.models,
                self.chat.provider.as_deref(),
                &self.chat.model,
            )
        });
        if current.as_ref() == Some(&model_key(model)) {
            "Current".to_string()
        } else if self.recent_models.contains(&model_key(model)) {
            "Recent".to_string()
        } else {
            let provider = if model.provider_display_name.trim().is_empty() {
                model.reference.provider.as_str()
            } else {
                model.provider_display_name.as_str()
            };
            format!("Provider: {provider}")
        }
    }

    fn commit_model_selection(&mut self, model: CatalogModel) {
        let key = model_key(&model);
        let provider_label = if model.provider_display_name.trim().is_empty() {
            model.reference.provider.clone()
        } else {
            model.provider_display_name.clone()
        };
        self.recent_models.retain(|candidate| candidate != &key);
        self.recent_models.push_front(key);
        self.recent_models.truncate(MAX_RECENT_MODELS);
        self.chat.model = model.reference.model.clone();
        self.chat.provider = Some(model.reference.provider.clone());
        self.model_picker = None;
        self.status_message = format!("Model: {} ({provider_label})", model.display_name);
    }
}

#[cfg(test)]
mod question_tests {
    use super::*;
    use bamboo_client_core::ToolResult;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    async fn read_test_http_request(stream: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;

        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            let read = stream.read(&mut chunk).await.unwrap();
            if read == 0 {
                break request.len();
            }
            request.extend_from_slice(&chunk[..read]);
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = stream.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    async fn read_test_http_path(stream: &mut tokio::net::TcpStream) -> String {
        read_test_http_request(stream)
            .await
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_string()
    }

    async fn respond_test_http(stream: &mut tokio::net::TcpStream, content_type: &str, body: &str) {
        use tokio::io::AsyncWriteExt;

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    async fn spawn_answer_test_server(
        session_id: &str,
    ) -> (BambooClient, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let expected_events_path = format!("/api/v1/events/{session_id}");
        let expected_respond_path = format!("/api/v1/respond/{session_id}");
        let server = tokio::spawn(async move {
            let (mut sse_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut sse_socket).await,
                expected_events_path
            );
            sse_socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            sse_socket.flush().await.unwrap();

            let (mut respond_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut respond_socket).await,
                expected_respond_path
            );
            respond_test_http(
                &mut respond_socket,
                "application/json",
                r#"{"auto_resume_status":"completed"}"#,
            )
            .await;
        });
        (BambooClient::new(&base_url), server)
    }

    fn question_identity(app: &App) -> QuestionIdentity {
        app.pending_question
            .as_ref()
            .expect("pending question")
            .identity()
    }

    fn active_question(
        session_id: &str,
        question: String,
        options: Option<Vec<String>>,
        tool_call_id: &str,
        allow_custom: bool,
        draft: &str,
    ) -> ActiveQuestion {
        let pending = PendingQuestion {
            has_pending_question: true,
            question,
            options,
            allow_custom,
            tool_call_id: Some(tool_call_id.to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            source: Some("pause_tool".to_string()),
        };
        ActiveQuestion::from_pending(session_id.to_string(), &pending, draft.to_string())
    }

    fn app_with_question(options: Vec<&str>) -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        let options: Vec<String> = options.into_iter().map(String::from).collect();
        app.pending_question = Some(active_question(
            "sess-1",
            "Run this command?".to_string(),
            Some(options),
            "tool-1",
            true,
            "",
        ));
        app
    }

    #[tokio::test]
    async fn option_navigation_and_custom_toggle() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);

        // Down moves the selection, clamped; Up moves back, clamped at 0.
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 1);
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 1); // clamped
        app.handle_question_key(key(KeyCode::Up)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 0);

        // `c` switches to free-text; typing fills the buffer; Esc returns to options.
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        assert!(app.pending_question.as_ref().unwrap().custom.is_some());
        app.handle_question_key(key(KeyCode::Char('h')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Char('i')))
            .await
            .unwrap();
        assert_eq!(
            app.pending_question.as_ref().unwrap().custom.as_deref(),
            Some("hi")
        );
        app.handle_question_key(key(KeyCode::Backspace))
            .await
            .unwrap();
        assert_eq!(
            app.pending_question.as_ref().unwrap().custom.as_deref(),
            Some("h")
        );
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().custom.is_none());
    }

    #[tokio::test]
    async fn esc_in_option_mode_dismisses() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_question.is_none());
    }

    #[tokio::test]
    async fn submitting_without_matching_session_keeps_the_question() {
        // The modal is bound to sess-1. If the active chat no longer matches,
        // keep the draft/question and refuse to send it anywhere.
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = None;
        assert!(app.chat.session_id.is_none());
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.is_some());
        assert!(!app.pending_question.as_ref().unwrap().submitting);
        assert!(app
            .notifications
            .last()
            .is_some_and(|notice| notice.text.contains("different session")));
    }

    #[tokio::test]
    async fn no_options_opens_in_free_text_mode() {
        let app = app_with_question(vec![]);
        assert!(app.pending_question.as_ref().unwrap().custom.is_some());
    }

    #[tokio::test]
    async fn closed_questions_never_expose_custom_input() {
        let mut app = app_with_question(vec!["One"]);
        app.pending_question.as_mut().unwrap().allow_custom = false;

        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();

        assert!(app.pending_question.as_ref().unwrap().custom.is_none());

        let closed_empty = active_question(
            "sess-1",
            "No choices".to_string(),
            None,
            "tool-closed",
            false,
            "must not open",
        );
        assert!(closed_empty.custom.is_none());
    }

    #[test]
    fn modal_hints_follow_allow_custom_at_narrow_width() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut closed = app_with_question(vec!["One", "Two"]);
        closed.pending_question.as_mut().unwrap().allow_custom = false;
        let backend = TestBackend::new(60, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &closed))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(!text.contains("custom"));
        assert!(text.contains("v inspect"));

        let open = app_with_question(vec!["One", "Two"]);
        terminal
            .draw(|frame| crate::ui::render(frame, &open))
            .unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(text.contains("c custom answer"));
    }

    #[test]
    fn http_and_sse_build_equivalent_typed_question_state() {
        let pending = PendingQuestion {
            has_pending_question: true,
            question: "Choose carefully".to_string(),
            options: Some(vec!["A".to_string(), "B".to_string()]),
            allow_custom: false,
            tool_call_id: Some("call-7".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            source: Some("pause_tool".to_string()),
        };
        let http = ActiveQuestion::from_pending("sess-7".to_string(), &pending, String::new());
        let sse = active_question(
            "sess-7",
            pending.question.clone(),
            pending.options.clone(),
            "call-7",
            pending.allow_custom,
            "",
        );

        assert_eq!(http.identity(), sse.identity());
        assert_eq!(http.tool_name, sse.tool_name);
        assert_eq!(http.source, sse.source);
        assert_eq!(http.options, sse.options);
        assert_eq!(http.allow_custom, sse.allow_custom);
        assert_eq!(http.custom, sse.custom);
    }

    #[tokio::test]
    async fn numeric_jump_reaches_and_submits_option_beyond_nine() {
        let options = (1..=12)
            .map(|number| format!("exact option {number}"))
            .collect::<Vec<_>>();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let (client, server) = spawn_answer_test_server("sess-1").await;
        app.client = client;
        app.chat.session_id = Some("sess-1".to_string());
        app.pending_question = Some(active_question(
            "sess-1",
            "Pick".to_string(),
            Some(options),
            "tool-12",
            false,
            "",
        ));

        app.handle_question_key(key(KeyCode::Char('g')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Char('1')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Char('2')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 11);

        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            AppEvent::AnswerSubmitted {
                answer,
                identity: QuestionIdentity {
                    tool_call_id: Some(tool_call_id),
                    ..
                },
                ..
            } if answer == "exact option 12" && tool_call_id == "tool-12"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_labels_remain_distinct_ordered_choices() {
        let mut app = app_with_question(vec!["Same", "Same", "Other"]);
        app.handle_question_key(key(KeyCode::Char('g')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Char('2')))
            .await
            .unwrap();
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();

        let question = app.pending_question.as_ref().unwrap();
        assert_eq!(question.options.len(), 3);
        assert_eq!(question.selected, 1);
        assert_eq!(question.options[0], question.options[1]);
    }

    #[test]
    fn long_unicode_question_and_option_are_inspectable_at_common_widths() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        for width in [60, 80, 120] {
            let question = format!(
                "START-问题🎋\n{}\nQUESTION_END",
                "长文本 🚀 wrapped words ".repeat(80)
            );
            let option = format!(
                "OPTION_START\n{}\nOPTION_END",
                "选项 🌟 wrapped words ".repeat(80)
            );
            let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
            app.pending_question = Some(active_question(
                "sess-1",
                question,
                Some(vec![option]),
                "tool-1",
                false,
                "",
            ));
            let backend = TestBackend::new(width, 24);
            let mut terminal = Terminal::new(backend).unwrap();

            app.pending_question.as_mut().unwrap().inspecting = true;
            app.pending_question.as_mut().unwrap().inspect_scroll = u16::MAX;
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(text.contains("QUESTION_END"), "width {width}");

            let question_state = app.pending_question.as_mut().unwrap();
            question_state.inspect_option = true;
            question_state.inspect_scroll = u16::MAX;
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let text: String = terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect();
            assert!(text.contains("OPTION_END"), "width {width}");
        }
    }

    #[tokio::test]
    async fn inspector_scroll_normalizes_after_terminal_width_increases() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.pending_question = Some(active_question(
            "sess-1",
            "wrapped question words ".repeat(160),
            Some(vec!["A".to_string()]),
            "tool-1",
            false,
            "",
        ));
        let question = app.pending_question.as_mut().unwrap();
        question.inspecting = true;
        question.inspect_scroll = u16::MAX;

        let mut narrow = Terminal::new(TestBackend::new(60, 24)).unwrap();
        narrow.draw(|frame| crate::ui::render(frame, &app)).unwrap();
        let narrow_max = app
            .pending_question
            .as_ref()
            .unwrap()
            .inspect_max_scroll
            .get();

        let mut wide = Terminal::new(TestBackend::new(120, 24)).unwrap();
        wide.draw(|frame| crate::ui::render(frame, &app)).unwrap();
        let wide_max = app
            .pending_question
            .as_ref()
            .unwrap()
            .inspect_max_scroll
            .get();
        assert!(wide_max < narrow_max);

        app.handle_question_key(key(KeyCode::Up)).await.unwrap();
        assert_eq!(
            app.pending_question.as_ref().unwrap().inspect_scroll,
            wide_max.saturating_sub(1)
        );
    }

    #[tokio::test]
    async fn replay_preserves_draft_selection_and_submission_state() {
        let mut app = app_with_question(vec!["A", "B"]);
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        for ch in "draft".chars() {
            app.handle_question_key(key(KeyCode::Char(ch)))
                .await
                .unwrap();
        }
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        app.pending_question.as_mut().unwrap().submitting = true;
        let epoch = app.answer_epoch;

        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "Run this command?".to_string(),
            options: Some(vec!["A".to_string(), "B".to_string()]),
            tool_call_id: Some("tool-1".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: true,
            source: Some("pause_tool".to_string()),
        })
        .unwrap();

        let replayed = app.pending_question.as_ref().unwrap();
        assert_eq!(app.answer_epoch, epoch);
        assert_eq!(replayed.selected, 1);
        assert_eq!(replayed.custom_draft, "draft");
        assert!(replayed.submitting);
    }

    #[tokio::test]
    async fn replay_of_a_dismissed_question_keeps_it_dismissed() {
        let mut app = app_with_question(vec!["A", "B"]);
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();

        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "Run this command?".to_string(),
            options: Some(vec!["Updated A".to_string(), "Updated B".to_string()]),
            tool_call_id: Some("tool-1".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: false,
            source: Some("pause_tool".to_string()),
        })
        .unwrap();

        assert!(app.pending_question.is_none());
        let dismissed = app.dismissed_question.as_ref().unwrap();
        assert_eq!(dismissed.options, ["Updated A", "Updated B"]);
        assert!(app.status_message.contains("remains dismissed"));
    }

    #[tokio::test]
    async fn a_new_question_discards_the_old_dismissed_cache() {
        let mut app = app_with_question(vec!["Old"]);
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();

        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "New question".to_string(),
            options: Some(vec!["New answer".to_string()]),
            tool_call_id: Some("tool-2".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: false,
            source: Some("pause_tool".to_string()),
        })
        .unwrap();
        assert!(app.dismissed_question.is_none());

        let identity = question_identity(&app);
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            identity,
            answer: "New answer".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        assert!(app.dismissed_question.is_none());

        app.reopen_pending_question();
        assert!(
            app.pending_question.is_none(),
            "Ctrl+Q must not resurrect the old consumed question"
        );
    }

    #[tokio::test]
    async fn custom_draft_survives_session_switch_and_reconnect() {
        let mut app = app_with_question(vec!["A", "B"]);
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        for ch in "keep me".chars() {
            app.handle_question_key(key(KeyCode::Char(ch)))
                .await
                .unwrap();
        }

        app.opening_session_id = Some("sess-2".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "sess-2".to_string(),
            result: Ok(OpenedSession {
                messages: Vec::new(),
                model: "model".to_string(),
                provider: None,
                project_id: None,
                is_running: false,
                pending: None,
                truncated: false,
                total_message_count: 0,
            }),
        })
        .await
        .unwrap();
        app.opening_session_id = Some("sess-1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "sess-1".to_string(),
            result: Ok(OpenedSession {
                messages: Vec::new(),
                model: "model".to_string(),
                provider: None,
                project_id: None,
                is_running: false,
                pending: Some(PendingQuestion {
                    has_pending_question: true,
                    question: "Run this command?".to_string(),
                    options: Some(vec!["A".to_string(), "B".to_string()]),
                    allow_custom: true,
                    tool_call_id: Some("tool-1".to_string()),
                    tool_name: Some("ConclusionWithOptions".to_string()),
                    source: Some("pause_tool".to_string()),
                }),
                truncated: false,
                total_message_count: 0,
            }),
        })
        .await
        .unwrap();

        assert_eq!(
            app.pending_question.as_ref().unwrap().custom_draft,
            "keep me"
        );
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        assert_eq!(
            app.pending_question.as_ref().unwrap().custom.as_deref(),
            Some("keep me")
        );
    }

    #[tokio::test]
    async fn legacy_custom_draft_migrates_after_exact_pending_identity_arrives() {
        let mut app = app_with_question(vec!["A", "B"]);
        let legacy = app.pending_question.as_mut().unwrap();
        legacy.tool_call_id = None;
        legacy.identity_syncing = true;
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        for ch in "保留这个答案".chars() {
            app.handle_question_key(key(KeyCode::Char(ch)))
                .await
                .unwrap();
        }

        app.opening_session_id = Some("sess-2".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "sess-2".to_string(),
            result: Ok(OpenedSession {
                messages: Vec::new(),
                model: "model".to_string(),
                provider: None,
                project_id: None,
                is_running: false,
                pending: None,
                truncated: false,
                total_message_count: 0,
            }),
        })
        .await
        .unwrap();

        app.opening_session_id = Some("sess-1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "sess-1".to_string(),
            result: Ok(OpenedSession {
                messages: Vec::new(),
                model: "model".to_string(),
                provider: None,
                project_id: None,
                is_running: false,
                pending: Some(PendingQuestion {
                    has_pending_question: true,
                    question: "Run this command?".to_string(),
                    options: Some(vec!["A".to_string(), "B".to_string()]),
                    allow_custom: true,
                    tool_call_id: Some("tool-1".to_string()),
                    tool_name: Some("ConclusionWithOptions".to_string()),
                    source: Some("pause_tool".to_string()),
                }),
                truncated: false,
                total_message_count: 0,
            }),
        })
        .await
        .unwrap();

        let restored = app.pending_question.as_ref().unwrap();
        assert_eq!(restored.tool_call_id.as_deref(), Some("tool-1"));
        assert_eq!(restored.custom_draft, "保留这个答案");
    }

    #[tokio::test]
    async fn legacy_custom_draft_does_not_cross_a_changed_question_contract() {
        let mut app = app_with_question(vec!["A", "B"]);
        let legacy = app.pending_question.as_mut().unwrap();
        legacy.tool_call_id = None;
        legacy.identity_syncing = true;
        legacy.custom_draft = "must stay with the old choices".to_string();

        app.opening_session_id = Some("sess-2".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "sess-2".to_string(),
            result: Ok(OpenedSession {
                messages: Vec::new(),
                model: "model".to_string(),
                provider: None,
                project_id: None,
                is_running: false,
                pending: None,
                truncated: false,
                total_message_count: 0,
            }),
        })
        .await
        .unwrap();

        app.opening_session_id = Some("sess-1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "sess-1".to_string(),
            result: Ok(OpenedSession {
                messages: Vec::new(),
                model: "model".to_string(),
                provider: None,
                project_id: None,
                is_running: false,
                pending: Some(PendingQuestion {
                    has_pending_question: true,
                    question: "Run this command?".to_string(),
                    options: Some(vec!["A".to_string(), "different".to_string()]),
                    allow_custom: true,
                    tool_call_id: Some("tool-new".to_string()),
                    tool_name: Some("ConclusionWithOptions".to_string()),
                    source: Some("pause_tool".to_string()),
                }),
                truncated: false,
                total_message_count: 0,
            }),
        })
        .await
        .unwrap();

        assert!(app
            .pending_question
            .as_ref()
            .unwrap()
            .custom_draft
            .is_empty());
    }

    #[test]
    fn question_draft_cache_is_bounded() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let oldest = QuestionDraftKey::new(
            "session-0".to_string(),
            Some("tool-0".to_string()),
            "question".to_string(),
            vec!["A".to_string()],
            true,
        );
        for index in 0..=MAX_QUESTION_DRAFTS {
            app.store_question_draft(
                QuestionDraftKey::new(
                    format!("session-{index}"),
                    Some(format!("tool-{index}")),
                    "question".to_string(),
                    vec!["A".to_string()],
                    true,
                ),
                format!("draft-{index}"),
            );
        }

        assert_eq!(app.question_drafts.len(), MAX_QUESTION_DRAFTS);
        assert_eq!(app.question_draft_order.len(), MAX_QUESTION_DRAFTS);
        assert!(!app.question_drafts.contains_key(&oldest));
    }

    #[tokio::test]
    async fn stale_pending_fetch_cannot_replace_another_session_question() {
        let mut app = app_with_question(vec!["Current"]);
        let original = app.pending_question.as_ref().unwrap().identity();
        app.handle_event(AppEvent::PendingQuestionChecked {
            session_id: "sess-old".to_string(),
            epoch: app.answer_epoch,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "Stale".to_string(),
                options: Some(vec!["Wrong".to_string()]),
                allow_custom: false,
                tool_call_id: Some("stale-tool".to_string()),
                ..Default::default()
            }),
        })
        .await
        .unwrap();

        assert_eq!(app.pending_question.as_ref().unwrap().identity(), original);
    }

    #[test]
    fn question_modal_renders_without_panicking() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let app = app_with_question(vec!["Approve", "Deny"]);
        let backend = TestBackend::new(60, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        // The rendered buffer should contain the question and an option label.
        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("Run this command?"), "question text missing");
        assert!(text.contains("Approve"), "option label missing");
    }

    #[tokio::test]
    async fn sse_clarification_stays_visible_and_owns_input_over_config_editor() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.config_editor = Some(ConfigEditor {
            textarea: tui_textarea::TextArea::new(vec!["CONFIG_EDITOR_SENTINEL".to_string()]),
        });
        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "VISIBLE_SECURITY_DECISION".to_string(),
            options: Some(vec!["Approve exact capability".to_string()]),
            tool_call_id: Some("tool-security".to_string()),
            tool_name: Some("request_permissions".to_string()),
            allow_custom: false,
            source: Some("pause_tool".to_string()),
        })
        .unwrap();

        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(text.contains("VISIBLE_SECURITY_DECISION"));
        assert!(text.contains("Clarification needed"));

        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.handle_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().submitting);
        assert!(app.config_editor.is_some(), "editor draft stays suspended");
    }

    /// Enter dispatches the answer POST off the event loop: the modal stays
    /// open in a submitting state with input disabled (no double-submit on
    /// repeated Enter, no dismissal out from under the request), and the
    /// spawned task posts exactly one `AnswerSubmitted` back through
    /// `event_tx`.
    #[tokio::test]
    async fn submit_dispatches_async_and_sets_in_flight() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        let (client, server) = spawn_answer_test_server("sess-1").await;
        app.client = client;
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();

        // The modal stays open, marked in flight — NOT cleared synchronously
        // (the old inline-await path cleared it only after the server reply).
        let q = app.pending_question.as_ref().expect("modal stays open");
        assert!(q.submitting, "submitting flag must be set");
        assert!(app.status_message.contains("Submitting"));

        // While in flight, every key is swallowed: navigation is frozen, a
        // repeated Enter is a no-op, and Esc cannot dismiss the modal.
        app.handle_question_key(key(KeyCode::Down)).await.unwrap();
        assert_eq!(app.pending_question.as_ref().unwrap().selected, 0);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(
            app.pending_question.is_some(),
            "Esc is disabled while submitting"
        );

        // The spawned task posts its result back after the fresh SSE handshake;
        // the dispatch and its epoch/answer payload are what's under test.
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("async result must be posted back")
            .expect("channel open");
        let AppEvent::AnswerSubmitted {
            epoch,
            answer,
            result,
            ..
        } = event
        else {
            panic!("expected AnswerSubmitted");
        };
        assert_eq!(answer, "Approve");
        assert_eq!(epoch, app.answer_epoch, "in-flight epoch is current");
        assert!(result.is_ok());
        // The swallowed repeat-Enter must not have dispatched a second POST.
        assert!(rx.try_recv().is_err(), "exactly one dispatch expected");
        server.await.unwrap();
    }

    /// A late `AnswerSubmitted` whose epoch no longer matches (the question
    /// was superseded mid-flight by a new one arriving over SSE) is discarded
    /// outright: it must neither clear the new modal nor flip streaming.
    #[tokio::test]
    async fn stale_epoch_answer_response_is_discarded() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        // Submit → the in-flight POST carries this epoch.
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let stale_epoch = app.answer_epoch;
        let stale_identity = question_identity(&app);

        // A NEW question arrives before the response lands — supersedes it.
        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "Second question?".to_string(),
            options: Some(vec!["A".to_string(), "B".to_string()]),
            tool_call_id: Some("tool-2".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: false,
            source: Some("pause_tool".to_string()),
        })
        .unwrap();
        assert_ne!(app.answer_epoch, stale_epoch, "supersede bumps the epoch");

        // The stale success response must be discarded, not applied.
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: stale_epoch,
            identity: stale_identity,
            answer: "Approve".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();

        let q = app.pending_question.as_ref().expect("new question stays");
        assert_eq!(q.question, "Second question?");
        assert!(!q.submitting, "the new question is not in flight");
        assert!(
            !app.chat.streaming,
            "stale response must not flip streaming"
        );

        // Session switch / run finalization also supersede an in-flight answer.
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let stale_epoch = app.answer_epoch;
        let stale_identity = question_identity(&app);
        app.finalize_streaming();
        assert_ne!(app.answer_epoch, stale_epoch);
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: stale_epoch,
            identity: stale_identity,
            answer: "A".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();
        assert!(!app.chat.streaming, "answer for a finalized run discarded");
    }

    /// A failed submit re-enables the modal (question kept, `submitting`
    /// cleared so Enter can retry) and surfaces the error through the
    /// notification overlay.
    #[tokio::test]
    async fn failed_submit_restores_question_and_notifies() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().submitting);
        let identity = question_identity(&app);

        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            identity,
            answer: "Approve".to_string(),
            result: Err(crate::api::RespondFailure::unavailable("boom")),
        })
        .await
        .unwrap();

        let q = app
            .pending_question
            .as_ref()
            .expect("question kept for retry");
        assert!(!q.submitting, "input re-enabled for retry");
        assert_eq!(q.error.as_deref(), Some("boom"));
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(last.text.contains("boom"));

        // And a retry dispatches again.
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.pending_question.as_ref().unwrap().submitting);
    }

    #[tokio::test]
    async fn stale_answer_rejection_refreshes_the_authoritative_question() {
        let mut app = app_with_question(vec!["Old"]);
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.pending_question.as_mut().unwrap().submitting = true;
        let old_identity = question_identity(&app);

        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            identity: old_identity.clone(),
            answer: "Old".to_string(),
            result: Err(crate::api::RespondFailure::rejected(
                reqwest::StatusCode::CONFLICT,
                "pending question changed".to_string(),
            )),
        })
        .await
        .unwrap();
        let refresh_epoch = app.answer_epoch;
        assert!(app.pending_question.as_ref().unwrap().submitting);

        app.handle_event(AppEvent::PendingQuestionRefreshed {
            session_id: "sess-1".to_string(),
            epoch: refresh_epoch,
            identity: old_identity,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "New question".to_string(),
                options: Some(vec!["New value".to_string()]),
                allow_custom: false,
                tool_call_id: Some("tool-2".to_string()),
                tool_name: Some("ConclusionWithOptions".to_string()),
                source: Some("pause_tool".to_string()),
            }),
        })
        .await
        .unwrap();

        let question = app.pending_question.as_ref().unwrap();
        assert_eq!(question.tool_call_id.as_deref(), Some("tool-2"));
        assert_eq!(question.options, ["New value"]);
        assert!(!question.submitting);
    }

    #[tokio::test]
    async fn consumed_answer_rejection_closes_the_stale_modal() {
        let mut app = app_with_question(vec!["Old"]);
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.pending_question.as_mut().unwrap().submitting = true;
        let identity = question_identity(&app);

        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            identity: identity.clone(),
            answer: "Old".to_string(),
            result: Err(crate::api::RespondFailure::rejected(
                reqwest::StatusCode::BAD_REQUEST,
                "no pending question".to_string(),
            )),
        })
        .await
        .unwrap();
        let refresh_epoch = app.answer_epoch;

        app.handle_event(AppEvent::PendingQuestionRefreshed {
            session_id: "sess-1".to_string(),
            epoch: refresh_epoch,
            identity,
            result: Ok(PendingQuestion::default()),
        })
        .await
        .unwrap();

        assert!(app.pending_question.is_none());
        assert!(app.status_message.contains("no longer exists"));
    }

    /// Success preserves the pre-existing post-submit semantics: the modal
    /// clears, and the `auto_resume_status` gate decides whether the spinner
    /// stays on (`started`/`already_running` — the SSE stream opened for the
    /// run is still attached) or streaming is finalized (any other status).
    #[tokio::test]
    async fn successful_submit_applies_auto_resume_gate() {
        // `started` → modal cleared, streaming on.
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let identity = question_identity(&app);
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            identity,
            answer: "Approve".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none(), "modal clears on success");
        assert!(app.chat.streaming, "resuming run keeps the spinner on");
        assert!(app.status_message.contains("resuming"));

        // `completed` (nothing resumed server-side) → modal cleared,
        // streaming finalized instead of spinning forever.
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("sess-1".to_string());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let identity = question_identity(&app);
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch: app.answer_epoch,
            identity,
            answer: "Approve".to_string(),
            result: Ok("completed".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        assert!(!app.chat.streaming, "non-resuming status must not spin");
    }

    #[tokio::test]
    async fn connected_reconciliation_cannot_erase_in_flight_completed_answer() {
        let mut app = app_with_question(vec!["Approve"]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.chat.streaming = true;
        app.pending_question.as_mut().unwrap().submitting = true;
        app.pending_reconcile_epoch = 9;
        let epoch = app.answer_epoch;
        let identity = question_identity(&app);

        app.handle_session_sse_event(SessionSseEvent::Connected {
            session_id: "sess-1".to_string(),
            stream_epoch: app.sse_epoch,
            reconnecting: false,
        })
        .unwrap();
        assert!(
            rx.try_recv().is_err(),
            "submitting handshake must not launch pending reconciliation"
        );

        // Even a response already queued by a racing request cannot clear the
        // modal or bump the answer epoch while the POST outcome owns it.
        app.handle_event(AppEvent::PendingQuestionReconciled {
            session_id: "sess-1".to_string(),
            epoch,
            reconcile_epoch: 9,
            result: Ok(PendingQuestion::default()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_some());
        assert_eq!(app.answer_epoch, epoch);

        app.handle_event(AppEvent::AnswerSubmitted {
            epoch,
            identity,
            answer: "Approve".to_string(),
            result: Ok("completed".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        assert!(!app.chat.streaming);
        assert_eq!(app.status_message, "Answered: Approve (completed)");
    }

    #[tokio::test]
    async fn legacy_question_cannot_submit_until_exact_identity_is_reconciled() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut sse_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut sse_socket).await,
                "/api/v1/events/sess-legacy"
            );
            sse_socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            sse_socket.flush().await.unwrap();

            let (mut respond_socket, _) = listener.accept().await.unwrap();
            let request = read_test_http_request(&mut respond_socket).await;
            assert!(request.starts_with("POST /api/v1/respond/sess-legacy "));
            assert!(
                request.contains(r#""expected_tool_call_id":"call-exact""#),
                "exact CAS identity missing from request: {request}"
            );
            assert!(
                request.contains(r#""response":"草稿🙂""#),
                "custom answer changed in request: {request}"
            );
            respond_test_http(
                &mut respond_socket,
                "application/json",
                r#"{"auto_resume_status":"completed"}"#,
            )
            .await;
        });

        let mut app = App::new(BambooClient::new(&base_url));
        app.chat.session_id = Some("sess-legacy".to_string());
        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "Legacy approval?".to_string(),
            options: Some(vec!["Approve".to_string(), "Deny".to_string()]),
            tool_call_id: None,
            tool_name: None,
            allow_custom: true,
            source: None,
        })
        .unwrap();
        assert!(app.pending_question.as_ref().unwrap().identity_syncing);

        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        for character in "草稿🙂".chars() {
            app.handle_question_key(key(KeyCode::Char(character)))
                .await
                .unwrap();
        }

        app.handle_question_key(key(KeyCode::Enter)).await.unwrap();
        let unresolved = app.pending_question.as_ref().unwrap();
        assert!(!unresolved.submitting);
        assert_eq!(unresolved.custom.as_deref(), Some("草稿🙂"));
        assert!(unresolved
            .error
            .as_deref()
            .is_some_and(|error| error.contains("not sent")));
        assert!(
            app.sse_task.is_none(),
            "no answer stream means no POST can start"
        );

        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.pending_reconcile_epoch = 7;
        let answer_epoch = app.answer_epoch;
        app.handle_event(AppEvent::PendingQuestionReconciled {
            session_id: "sess-legacy".to_string(),
            epoch: answer_epoch,
            reconcile_epoch: 7,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "Legacy approval?".to_string(),
                options: Some(vec!["Approve".to_string(), "Deny".to_string()]),
                allow_custom: true,
                tool_call_id: Some("call-exact".to_string()),
                tool_name: Some("ConclusionWithOptions".to_string()),
                source: Some("pause_tool".to_string()),
            }),
        })
        .await
        .unwrap();

        let resolved = app.pending_question.as_ref().unwrap();
        assert_eq!(resolved.tool_call_id.as_deref(), Some("call-exact"));
        assert!(!resolved.identity_syncing);
        assert_eq!(resolved.custom.as_deref(), Some("草稿🙂"));
        app.submit_answer("草稿🙂".to_string());
        assert!(app.pending_question.as_ref().unwrap().submitting);
        assert!(app.sse_task.is_some());

        let submitted = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("answer result must return")
            .expect("event channel must stay open");
        app.handle_event(submitted).await.unwrap();
        assert!(app.pending_question.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stopping_aborts_answer_waiting_for_sse_before_it_can_post() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (sse_seen_tx, sse_seen_rx) = tokio::sync::oneshot::channel();
        let (release_sse_tx, release_sse_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut sse_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut sse_socket).await,
                "/api/v1/events/sess-1"
            );
            sse_seen_tx.send(()).ok();
            release_sse_rx.await.unwrap();
            sse_socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            sse_socket.flush().await.unwrap();

            let (mut stop_socket, _) = listener.accept().await.unwrap();
            let stop_path = read_test_http_path(&mut stop_socket).await;
            assert_eq!(stop_path, "/api/v1/stop/sess-1");
            respond_test_http(
                &mut stop_socket,
                "application/json",
                r#"{"status":"stopped"}"#,
            )
            .await;

            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(300), listener.accept())
                    .await
                    .is_err(),
                "aborted answer task must not POST after SSE becomes ready"
            );
        });

        let mut app = app_with_question(vec!["Approve"]);
        app.client = BambooClient::new(&base_url);
        app.chat.streaming = true;
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(event_tx);

        app.submit_answer("Approve".to_string());
        sse_seen_rx.await.unwrap();
        assert!(app.answer_task.is_some());
        assert!(app.pending_question.as_ref().unwrap().submitting);

        app.stop_streaming();
        assert!(
            app.answer_task.is_none(),
            "stop must abort the network task"
        );
        release_sse_tx.send(()).unwrap();

        let stop_finished =
            tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv())
                .await
                .expect("stop result must return")
                .expect("event channel must stay open");
        assert!(matches!(stop_finished, AppEvent::StopFinished(Ok(()))));
        app.handle_event(stop_finished).await.unwrap();
        assert!(!app.chat.streaming);
        assert!(app.pending_question.is_none());
        server.await.unwrap();
    }

    /// While the POST is out, the modal renders the submitting state instead
    /// of the interactive key hints.
    #[test]
    fn question_modal_shows_submitting_state() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.pending_question.as_mut().unwrap().submitting = true;
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(
            text.contains("Submitting answer"),
            "submitting hint missing"
        );
        assert!(
            !text.contains("Enter answer"),
            "interactive hints must be hidden while in flight"
        );
    }

    #[test]
    fn many_options_window_keeps_selection_visible() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let opts: Vec<String> = (1..=30).map(|n| format!("opt{n}")).collect();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let mut question = active_question(
            "sess-1",
            "Pick one".to_string(),
            Some(opts),
            "tool-1",
            false,
            "",
        );
        question.selected = 24; // "25. opt25", deep in the list
        app.pending_question = Some(question);

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();
        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();

        // The selected option is visible even though it's far down the list, and
        // an overflow marker indicates hidden options above.
        assert!(text.contains("opt25"), "selected option not in the window");
        assert!(text.contains("more"), "overflow marker missing");
    }

    #[tokio::test]
    async fn mouse_click_submits_the_exact_option_value() {
        use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let options = (1..=30)
            .map(|index| format!("exact option {index}"))
            .collect::<Vec<_>>();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let (client, server) = spawn_answer_test_server("sess-1").await;
        app.client = client;
        app.chat.session_id = Some("sess-1".to_string());
        app.pending_question = Some(active_question(
            "sess-1",
            "Pick one".to_string(),
            Some(options),
            "tool-1",
            false,
            "",
        ));
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let hitbox = *app
            .pending_question
            .as_ref()
            .unwrap()
            .option_hitboxes
            .borrow()
            .last()
            .unwrap();
        let expected = app.pending_question.as_ref().unwrap().options[hitbox.index].clone();
        let mouse = |kind| MouseEvent {
            kind,
            column: hitbox.x,
            row: hitbox.y,
            modifiers: KeyModifiers::empty(),
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_mouse(mouse(MouseEventKind::Down(MouseButton::Left)));
        assert_eq!(
            app.pending_question.as_ref().unwrap().selected,
            0,
            "mouse-down must not recenter a long option window"
        );
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        app.handle_mouse(mouse(MouseEventKind::Up(MouseButton::Left)));

        let event = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            AppEvent::AnswerSubmitted { answer, .. } if answer == expected
        ));
        server.await.unwrap();
    }

    fn bare_session(id: &str) -> SessionSummary {
        SessionSummary {
            id: id.into(),
            project_id: None,
            title: String::new(),
            title_generated: true,
            model: String::new(),
            model_ref: None,
            provider: None,
            is_running: false,
            has_pending_question: false,
            last_run_status: None,
            updated_at: None,
            message_count: 0,
            pinned: false,
        }
    }

    #[tokio::test]
    async fn loaded_event_applies_to_state_and_clears_loading() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.sessions.loading = true;
        app.handle_event(AppEvent::SessionsLoaded(Ok(ListSessionsEnvelope {
            sessions: vec![bare_session("s1")],
            total: 1,
            limit: 200,
            offset: 0,
            next_offset: None,
        })))
        .await
        .unwrap();
        assert!(!app.sessions.loading, "loading flag must clear");
        assert_eq!(app.sessions.sessions.len(), 1);
        assert_eq!(app.sessions.total, 1);
        assert_eq!(app.sessions.page_limit, 200);
        assert!(app.sessions.next_offset.is_none());
        assert!(app.sessions.error.is_none());

        // Error result surfaces on the tab and clears loading.
        app.sessions.loading = true;
        app.handle_event(AppEvent::SessionsLoaded(Err("boom".into())))
            .await
            .unwrap();
        assert!(!app.sessions.loading);
        assert_eq!(app.sessions.error.as_deref(), Some("boom"));
    }

    /// `]` advances to the next page only when the server reported one; `[`
    /// steps back by a full page and clamps at 0. Selection resets on every
    /// page change.
    #[tokio::test]
    async fn sessions_paging_keys_respect_next_offset_and_clamp() {
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.sessions.sessions = vec![bare_session("s1"), bare_session("s2")];
        app.sessions.selected = 1;
        app.sessions.total = 5;
        app.sessions.page_limit = 2;
        app.sessions.offset = 0;
        app.sessions.next_offset = Some(2);

        // No event_tx wired (tests construct App directly), so `load_tab_data`
        // is a no-op past the state mutation — enough to assert the key logic.
        app.handle_sessions_key(k(KeyCode::Char(']')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 2, "] advances by the reported page");
        assert_eq!(app.sessions.selected, 0, "selection resets on page change");

        // On the last page (`next_offset` is `None`), `]` is a no-op.
        app.sessions.next_offset = None;
        app.sessions.selected = 1;
        app.handle_sessions_key(k(KeyCode::Char(']')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 2, "] does nothing past the last page");
        assert_eq!(app.sessions.selected, 1, "no page change, no reset");

        // `[` steps back by a full page.
        app.handle_sessions_key(k(KeyCode::Char('[')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 0);
        assert_eq!(app.sessions.selected, 0);

        // Already at offset 0: `[` clamps and does not go negative (saturating).
        app.sessions.selected = 1;
        app.handle_sessions_key(k(KeyCode::Char('[')))
            .await
            .unwrap();
        assert_eq!(app.sessions.offset, 0);
        assert_eq!(
            app.sessions.selected, 1,
            "no page change at offset 0, no reset"
        );
    }

    /// `d` on the Sessions tab opens a confirmation modal instead of deleting
    /// immediately; `Esc`/`n` cancel it without touching `event_tx`.
    #[tokio::test]
    async fn sessions_delete_opens_confirm_modal_and_cancel_clears_it() {
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.sessions.sessions = vec![bare_session("s1")];
        app.sessions.selected = 0;

        app.handle_key(k(KeyCode::Char('d'))).await.unwrap();
        assert_eq!(
            app.pending_delete.as_ref().map(|(id, _)| id.as_str()),
            Some("s1")
        );

        // While the modal is open, keys route to it — not tab switching.
        app.handle_key(k(KeyCode::Char('1'))).await.unwrap();
        assert_eq!(app.tab, Tab::Sessions, "digit must not switch tabs");
        assert!(app.pending_delete.is_some());

        app.handle_key(k(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_delete.is_none(), "Esc cancels");
    }

    /// Confirming (`y`) clears the modal and posts the delete off the event
    /// loop; without `run()` having wired `event_tx`, there's nothing to spawn
    /// into, so this only asserts the modal itself is cleared synchronously.
    #[tokio::test]
    async fn sessions_delete_confirm_clears_modal() {
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.pending_delete = Some(("s1".to_string(), "My session".to_string()));
        app.handle_key(k(KeyCode::Char('y'))).await.unwrap();
        assert!(app.pending_delete.is_none());
    }

    #[tokio::test]
    async fn successful_delete_of_active_session_resets_server_backed_chat_state() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("active".to_string());
        app.chat.project_id = Some("project-1".to_string());
        app.chat.model = "gpt-5".to_string();
        app.chat.provider = Some("openai".to_string());
        app.chat.messages.push(ChatMessage {
            role: MessageRole::Assistant,
            content: "old transcript".to_string(),
            tool_calls: Vec::new(),
            reasoning: None,
        });
        app.chat.current_response = "partial".to_string();
        app.chat.streaming = true;
        app.chat.textarea.input(key(KeyCode::Char('d')));
        app.deleting_session_id = Some("active".to_string());

        app.handle_event(AppEvent::SessionDeleted {
            session_id: "active".to_string(),
            result: Ok(()),
            session_picker_epoch: None,
        })
        .await
        .unwrap();

        assert!(app.deleting_session_id.is_none());
        assert!(app.chat.session_id.is_none());
        assert!(app.chat.messages.is_empty());
        assert!(app.chat.current_response.is_empty());
        assert!(!app.chat.streaming);
        assert_eq!(app.chat.model, "gpt-5");
        assert_eq!(app.chat.provider.as_deref(), Some("openai"));
        assert_eq!(app.chat.project_id.as_deref(), Some("project-1"));
        assert_eq!(app.chat.textarea.lines().join("\n"), "d");
        assert_eq!(
            app.status_message,
            "Session deleted — started a new session"
        );
    }

    #[tokio::test]
    async fn successful_delete_of_opening_non_active_session_preserves_current_chat() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("active".to_string());
        app.chat.messages.push(ChatMessage {
            role: MessageRole::Assistant,
            content: "keep transcript".to_string(),
            tool_calls: Vec::new(),
            reasoning: None,
        });
        app.opening_session_id = Some("delete-me".to_string());
        app.deleting_session_id = Some("delete-me".to_string());

        app.handle_event(AppEvent::SessionDeleted {
            session_id: "delete-me".to_string(),
            result: Ok(()),
            session_picker_epoch: None,
        })
        .await
        .unwrap();

        assert!(app.deleting_session_id.is_none());
        assert!(app.opening_session_id.is_none());
        assert_eq!(app.chat.session_id.as_deref(), Some("active"));
        assert_eq!(app.chat.messages[0].content, "keep transcript");

        app.handle_event(AppEvent::SessionOpened {
            session_id: "delete-me".to_string(),
            result: Ok(opened(vec![asst_msg("must stay stale")])),
        })
        .await
        .unwrap();
        assert_eq!(app.chat.session_id.as_deref(), Some("active"));
        assert_eq!(app.chat.messages[0].content, "keep transcript");
    }

    #[tokio::test]
    async fn active_session_delete_is_blocked_while_chat_is_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("active".to_string());
        app.chat.streaming = true;
        app.pending_delete = Some(("active".to_string(), "Active".to_string()));
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(event_tx);

        app.handle_delete_confirm_key(key(KeyCode::Enter))
            .await
            .unwrap();

        assert!(app.pending_delete.is_none());
        assert!(app.deleting_session_id.is_none());
        assert!(event_rx.try_recv().is_err());
        assert!(app.status_message.contains("Stop the active run"));
    }

    #[tokio::test]
    async fn enter_keeps_draft_while_active_session_delete_is_in_flight() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("active".to_string());
        app.deleting_session_id = Some("active".to_string());
        for character in "keep me".chars() {
            app.chat.textarea.input(key(KeyCode::Char(character)));
        }

        app.handle_chat_key(key(KeyCode::Enter)).await.unwrap();

        assert_eq!(app.chat.textarea.lines().join("\n"), "keep me");
        assert!(app.chat.messages.is_empty());
        assert!(!app.chat.streaming);
        assert_eq!(
            app.status_message,
            "Session is being deleted — message kept as draft"
        );
    }

    /// F1/Ctrl+L must not stack the help/notification overlay on top of an
    /// already-open modal — `any_modal_open` gates them so the keystroke
    /// falls through to the modal's own handler instead (a no-op there,
    /// since neither key means anything to the delete-confirm modal).
    #[tokio::test]
    async fn f1_and_ctrl_l_are_suppressed_while_a_modal_is_open() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.pending_delete = Some(("s1".to_string(), "My session".to_string()));

        app.handle_key(KeyEvent::new(KeyCode::F(1), KeyModifiers::empty()))
            .await
            .unwrap();
        assert!(!app.help_visible, "F1 must not open help over a modal");
        assert!(app.pending_delete.is_some(), "the modal must stay open");

        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(
            !app.notifications_visible,
            "Ctrl+L must not open the log over a modal"
        );
        assert!(app.pending_delete.is_some(), "the modal must stay open");
    }

    #[test]
    fn mouse_wheel_scrolls_chat() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.scroll_offset = 10;
        // Simulates a render having already established the bound (a real
        // frame always renders before the first input is handled).
        app.chat.max_scroll.set(50);
        let ev = |k| MouseEvent {
            kind: k,
            column: 0,
            row: 0,
            modifiers: crossterm::event::KeyModifiers::empty(),
        };
        app.handle_mouse(ev(MouseEventKind::ScrollUp));
        assert_eq!(app.chat.scroll_offset, 7);
        assert!(!app.chat.auto_scroll);
        app.handle_mouse(ev(MouseEventKind::ScrollDown));
        assert_eq!(app.chat.scroll_offset, 10);
    }

    /// Scrolling down is clamped to `max_scroll` (set by the render function
    /// each frame): spamming `j` past the bottom must not overshoot into a
    /// dead zone that then eats several `k` presses before the view moves.
    #[tokio::test]
    async fn chat_scroll_down_clamps_to_max_scroll() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.max_scroll.set(5);

        for _ in 0..50 {
            app.chat_scroll_down(3);
        }
        assert_eq!(
            app.chat.scroll_offset, 5,
            "scrolling down must clamp at max_scroll, not grow unbounded"
        );

        // One `k` must immediately move the view (not be swallowed catching
        // up from an overshot offset).
        app.handle_key(k(KeyCode::Char('k'))).await.unwrap();
        assert_eq!(app.chat.scroll_offset, 2);
    }

    #[test]
    fn schedule_form_cycles_and_validates() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        fn k(c: KeyCode) -> KeyEvent {
            KeyEvent::new(c, KeyModifiers::empty())
        }

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.schedule_form = Some(ScheduleForm::default());

        // Type into the name field, then Tab to cron.
        app.handle_schedule_form_key(k(KeyCode::Char('h')));
        app.handle_schedule_form_key(k(KeyCode::Char('i')));
        assert_eq!(app.schedule_form.as_ref().unwrap().name, "hi");
        app.handle_schedule_form_key(k(KeyCode::Tab));
        assert_eq!(app.schedule_form.as_ref().unwrap().field, 1);

        // Enter with empty cron/prompt does NOT submit (form stays open).
        app.handle_schedule_form_key(k(KeyCode::Enter));
        assert!(
            app.schedule_form.is_some(),
            "incomplete form must not submit"
        );

        // Esc cancels.
        app.handle_schedule_form_key(k(KeyCode::Esc));
        assert!(app.schedule_form.is_none());
    }

    /// Regression: while the form is open, `handle_key` must route Tab and the
    /// 1-6 digit keys to the form instead of switching app tabs (cron
    /// expressions are full of digits, Tab moves between fields).
    #[tokio::test]
    async fn schedule_form_captures_keys_through_handle_key() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Schedules;
        app.handle_key(k(KeyCode::Char('n'))).await.unwrap();
        assert!(app.schedule_form.is_some(), "`n` opens the form");

        // A digit must be typed into the focused field, not switch to tab N.
        app.handle_key(k(KeyCode::Char('1'))).await.unwrap();
        assert_eq!(app.tab, Tab::Schedules, "digit must not switch tabs");
        assert_eq!(app.schedule_form.as_ref().unwrap().name, "1");

        // Tab must advance the form field, not switch app tab.
        app.handle_key(k(KeyCode::Tab)).await.unwrap();
        assert_eq!(app.tab, Tab::Schedules, "Tab must not switch tabs");
        assert_eq!(app.schedule_form.as_ref().unwrap().field, 1);
    }

    #[tokio::test]
    async fn config_editor_opens_validates_and_cancels() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let plain = |c| KeyEvent::new(c, KeyModifiers::empty());
        let ctrl_s = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL);

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Config;

        // `e` with no config loaded just posts a status, opens nothing.
        app.handle_key(plain(KeyCode::Char('e'))).await.unwrap();
        assert!(app.config_editor.is_none());

        // With a config, `e` opens the editor prefilled.
        app.config.config = Some(serde_json::json!({ "a": 1 }));
        app.handle_key(plain(KeyCode::Char('e'))).await.unwrap();
        assert!(app.config_editor.is_some());

        // Corrupt the buffer (prepend a stray char) → Ctrl+S must NOT close it.
        app.handle_key(plain(KeyCode::Char('x'))).await.unwrap();
        app.handle_key(ctrl_s).await.unwrap();
        assert!(
            app.config_editor.is_some(),
            "invalid JSON must keep the editor open"
        );
        assert!(app.status_message.contains("Invalid JSON"));

        // Esc cancels.
        app.handle_key(plain(KeyCode::Esc)).await.unwrap();
        assert!(app.config_editor.is_none());

        // A clean open + immediate Ctrl+S (buffer is valid) closes the editor.
        app.handle_key(plain(KeyCode::Char('e'))).await.unwrap();
        assert!(app.config_editor.is_some());
        app.handle_key(ctrl_s).await.unwrap();
        assert!(app.config_editor.is_none(), "valid JSON saves and closes");
    }

    #[tokio::test]
    async fn notifications_log_and_badge() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        // An error records to the log, mirrors to the status line, bumps badge.
        app.notify(NoticeLevel::Error, "boom");
        assert_eq!(app.notifications.len(), 1);
        assert_eq!(app.status_message, "boom");
        assert_eq!(app.unseen_alerts, 1);

        // Info records but does not bump the alert badge.
        app.notify(NoticeLevel::Info, "just fyi");
        assert_eq!(app.unseen_alerts, 1);
        assert_eq!(app.notifications.len(), 2);

        // Ctrl+L opens the log and clears the badge.
        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.notifications_visible);
        assert_eq!(app.unseen_alerts, 0);

        // Any key dismisses the overlay.
        app.handle_event(AppEvent::Key(KeyEvent::new(
            KeyCode::Char('q'),
            KeyModifiers::empty(),
        )))
        .await
        .unwrap();
        assert!(!app.notifications_visible);
    }

    #[test]
    fn notifications_log_is_capped() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        for i in 0..250 {
            app.notify(NoticeLevel::Info, format!("n{i}"));
        }
        assert_eq!(app.notifications.len(), 200, "log is capped at 200");
        // Oldest entries are dropped; the newest is retained.
        assert_eq!(app.notifications.last().unwrap().text, "n249");
        assert_eq!(app.notifications.first().unwrap().text, "n50");
    }

    #[tokio::test]
    async fn subagent_lifecycle_tracks_children() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::SubAgentStarted {
            child_session_id: "c1".into(),
            title: Some("research".into()),
        })
        .unwrap();
        assert_eq!(app.chat.sub_agents.len(), 1);
        assert_eq!(app.chat.sub_agents[0].status, "running");

        // Duplicate start is ignored.
        app.handle_sse_event(AgentEvent::SubAgentStarted {
            child_session_id: "c1".into(),
            title: None,
        })
        .unwrap();
        assert_eq!(app.chat.sub_agents.len(), 1);

        app.handle_sse_event(AgentEvent::SubAgentCompleted {
            child_session_id: "c1".into(),
            status: "completed".into(),
            error: None,
        })
        .unwrap();
        assert_eq!(app.chat.sub_agents[0].status, "completed");
    }

    /// Parallel tool calls: a `ToolComplete` must land on the entry whose
    /// `tool_call_id` it names, not on whichever entry is last in the list.
    #[tokio::test]
    async fn tool_events_pair_by_id_not_position() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::ToolStart {
            tool_call_id: "a".into(),
            tool_name: "Read".into(),
            arguments: serde_json::json!({}),
        })
        .unwrap();
        app.handle_sse_event(AgentEvent::ToolStart {
            tool_call_id: "b".into(),
            tool_name: "Write".into(),
            arguments: serde_json::json!({}),
        })
        .unwrap();

        // "b" was started most recently (last in the list), but the Complete
        // event names "a" — position-based pairing would wrongly land this on
        // "b".
        app.handle_sse_event(AgentEvent::ToolComplete {
            tool_call_id: "a".into(),
            result: ToolResult {
                success: true,
                result: "a-result".into(),
            },
        })
        .unwrap();

        let calls = &app.chat.current_tool_calls;
        assert_eq!(calls.len(), 2, "no entry is dropped or duplicated");
        let a = calls.iter().find(|t| t.id == "a").unwrap();
        let b = calls.iter().find(|t| t.id == "b").unwrap();
        assert_eq!(a.result.as_deref(), Some("a-result"));
        assert_eq!(a.phase, "complete");
        assert!(b.result.is_none(), "b's result must be untouched");
        assert_eq!(b.phase, "running", "b must still be running");
    }

    /// A `ToolComplete`/`ToolError` for an id with no matching `ToolStart` is
    /// surfaced defensively (not silently dropped).
    #[tokio::test]
    async fn tool_complete_for_unknown_id_inserts_defensively() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::ToolComplete {
            tool_call_id: "ghost".into(),
            result: ToolResult {
                success: true,
                result: "surprise".into(),
            },
        })
        .unwrap();

        assert_eq!(app.chat.current_tool_calls.len(), 1);
        let tc = &app.chat.current_tool_calls[0];
        assert_eq!(tc.id, "ghost");
        assert_eq!(tc.tool_name, "unknown");
        assert_eq!(tc.result.as_deref(), Some("surprise"));
        assert_eq!(tc.phase, "complete");
    }

    /// `AppEvent::ExecuteFailed` (posted when the `execute` POST itself fails)
    /// must clear `chat.streaming` even though no SSE terminal event ever
    /// arrived for the run.
    #[tokio::test]
    async fn execute_failed_event_clears_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.current_response = "partial".to_string();

        app.handle_event(AppEvent::ExecuteFailed("connection refused".to_string()))
            .await
            .unwrap();

        assert!(
            !app.chat.streaming,
            "streaming must clear on execute failure"
        );
        // notify() runs before finalize_streaming() (mirroring the existing
        // AgentEvent::Error handler), so the transient status line ends up
        // "Ready" — the failure detail is preserved in the notification log
        // instead, which is what Ctrl+L surfaces.
        let last = app.notifications.last().expect("notify logged an entry");
        assert!(last.text.contains("connection refused"));
        assert_eq!(last.level, NoticeLevel::Error);
    }

    /// `StopFinished(Err)` must still finalize streaming locally so the
    /// operator regains control of the input even when the stop request
    /// itself failed (e.g. the server is unreachable) — `App::running` stays
    /// `true` (the app itself does not exit).
    #[tokio::test]
    async fn stop_failure_still_finalizes_and_keeps_app_running() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.session_id = Some("s1".to_string());

        app.handle_event(AppEvent::StopFinished(
            Err("server unreachable".to_string()),
        ))
        .await
        .unwrap();

        assert!(
            !app.chat.streaming,
            "streaming must clear despite the error"
        );
        assert!(app.running, "a failed stop must not tear down the app");
        assert!(app.status_message.contains("server unreachable"));
    }

    /// `StopFinished(Ok)` finalizes streaming and reports "Stopped".
    #[tokio::test]
    async fn stop_success_finalizes_with_stopped_status() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.session_id = Some("s1".to_string());

        app.handle_event(AppEvent::StopFinished(Ok(())))
            .await
            .unwrap();

        assert!(!app.chat.streaming);
        assert_eq!(app.status_message, "Stopped");
    }

    // ── Session resume (WP3) ──

    fn opened(messages: Vec<ChatMessage>) -> OpenedSession {
        OpenedSession {
            messages,
            model: "claude-sonnet-5".to_string(),
            provider: None,
            project_id: None,
            is_running: false,
            pending: None,
            truncated: false,
            total_message_count: 0,
        }
    }

    fn asst_msg(content: &str) -> ChatMessage {
        ChatMessage {
            role: MessageRole::Assistant,
            content: content.to_string(),
            tool_calls: Vec::new(),
            reasoning: None,
        }
    }

    /// A successful resume installs the mapped history, the model, and the
    /// session id; switches to the Chat tab; and leaves `streaming` false
    /// when the session isn't currently running.
    #[tokio::test]
    async fn session_opened_installs_state_and_switches_tab() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        // Leftover scratch state from a previous session — must be wiped.
        app.chat.current_response = "stale partial".to_string();
        app.chat.token_usage = Some(TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
        });

        app.opening_session_id = Some("s1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(opened(vec![asst_msg("hello again")])),
        })
        .await
        .unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("s1"));
        assert_eq!(app.chat.model, "claude-sonnet-5");
        assert_eq!(app.chat.messages.len(), 1);
        assert_eq!(app.tab, Tab::Chat);
        assert!(app.chat.auto_scroll);
        assert!(
            !app.chat.streaming,
            "is_running: false must not start streaming"
        );
        assert!(app.chat.current_response.is_empty(), "scratch state wiped");
        assert!(app.chat.token_usage.is_none(), "scratch state wiped");
    }

    /// The session picker closes before its asynchronous resume finishes. A
    /// plain Enter during that gap must keep the composer draft intact instead
    /// of starting a chat against the previously visible session.
    #[tokio::test]
    async fn chat_enter_during_session_resume_preserves_draft_and_does_not_send() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("session-a".to_string());
        app.opening_session_id = Some("session-b".to_string());
        for character in "send after resume".chars() {
            app.chat.textarea.input(key(KeyCode::Char(character)));
        }

        app.handle_chat_key(key(KeyCode::Enter)).await.unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("session-a"));
        assert_eq!(app.opening_session_id.as_deref(), Some("session-b"));
        assert_eq!(app.chat.textarea.lines().join("\n"), "send after resume");
        assert!(app.chat.messages.is_empty());
        assert!(!app.chat.streaming);
        assert_eq!(
            app.status_message,
            "Session is still resuming — message kept as draft"
        );
    }

    #[tokio::test]
    async fn out_of_order_session_open_result_is_ignored() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("current".to_string());
        app.opening_session_id = Some("newest".to_string());

        app.handle_event(AppEvent::SessionOpened {
            session_id: "older".to_string(),
            result: Ok(opened(vec![asst_msg("stale")])),
        })
        .await
        .unwrap();
        assert_eq!(app.chat.session_id.as_deref(), Some("current"));
        assert_eq!(app.opening_session_id.as_deref(), Some("newest"));

        app.handle_event(AppEvent::SessionOpened {
            session_id: "newest".to_string(),
            result: Ok(opened(vec![asst_msg("fresh")])),
        })
        .await
        .unwrap();
        assert_eq!(app.chat.session_id.as_deref(), Some("newest"));
        assert_eq!(app.chat.messages[0].content, "fresh");
        assert!(app.opening_session_id.is_none());
    }

    #[tokio::test]
    async fn opening_a_stopped_session_detaches_the_previous_stream() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("old".to_string());
        let (sse_tx, sse_rx) = mpsc::unbounded_channel::<SessionSseEvent>();
        let old_tx = sse_tx.clone();
        app.sse_tx = Some(sse_tx);
        app.sse_rx = Some(sse_rx);
        app.sse_task = Some(tokio::spawn(std::future::pending()));
        app.opening_session_id = Some("new".to_string());

        app.handle_event(AppEvent::SessionOpened {
            session_id: "new".to_string(),
            result: Ok(opened(vec![])),
        })
        .await
        .unwrap();

        assert!(app.sse_tx.is_none());
        assert!(app.sse_rx.is_none());
        assert!(app.sse_task.is_none());
        assert!(
            old_tx.is_closed(),
            "the old SSE sender must lose its receiver"
        );
    }

    #[test]
    fn stale_session_sse_event_cannot_mutate_the_current_session() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("new".to_string());
        let clarification = || AgentEvent::NeedClarification {
            question: "Old session question".to_string(),
            options: Some(vec!["Wrong".to_string()]),
            tool_call_id: Some("old-tool".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: false,
            source: Some("pause_tool".to_string()),
        };

        app.handle_session_sse_event(SessionSseEvent::Event {
            session_id: "old".to_string(),
            stream_epoch: app.sse_epoch,
            event: clarification(),
        })
        .unwrap();
        assert!(app.pending_question.is_none());

        app.handle_session_sse_event(SessionSseEvent::Event {
            session_id: "new".to_string(),
            stream_epoch: app.sse_epoch,
            event: clarification(),
        })
        .unwrap();
        assert!(app.pending_question.is_some());
    }

    #[tokio::test]
    async fn transport_failure_preserves_question_draft_and_answer_reattaches_stream() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.streaming = true;
        app.connected = true;
        app.handle_question_key(key(KeyCode::Char('c')))
            .await
            .unwrap();
        for ch in "keep this draft".chars() {
            app.handle_question_key(key(KeyCode::Char(ch)))
                .await
                .unwrap();
        }
        let identity = question_identity(&app);
        let epoch = app.answer_epoch;
        let (sse_tx, sse_rx) = mpsc::unbounded_channel();
        app.sse_tx = Some(sse_tx);
        app.sse_rx = Some(sse_rx);
        app.sse_task = Some(tokio::spawn(std::future::pending()));

        app.handle_session_sse_event(SessionSseEvent::TransportFailed {
            session_id: "sess-1".to_string(),
            stream_epoch: app.sse_epoch,
            message: "retry budget exhausted".to_string(),
        })
        .unwrap();

        let question = app.pending_question.as_ref().expect("question retained");
        assert_eq!(question.custom.as_deref(), Some("keep this draft"));
        assert_eq!(app.answer_epoch, epoch);
        assert!(
            app.chat.streaming,
            "unknown server run stays input-blocking"
        );
        assert!(app.stream_disconnected);
        assert!(!app.connected);
        assert!(app.sse_task.is_none());

        app.handle_event(AppEvent::AnswerSubmitted {
            epoch,
            identity,
            answer: "keep this draft".to_string(),
            result: Ok("started".to_string()),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        assert!(app.chat.streaming);
        assert!(app.sse_task.is_some(), "successful answer reattaches SSE");
        app.detach_stream();
    }

    #[tokio::test]
    async fn transport_failure_keeps_unknown_active_run_input_blocked_and_partial_output() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-transport".to_string());
        app.chat.streaming = true;
        app.chat.current_response = "partial 你好".to_string();
        app.chat.textarea.input(key(KeyCode::Char('n')));
        app.chat.textarea.input(key(KeyCode::Char('e')));
        app.chat.textarea.input(key(KeyCode::Char('w')));
        let (sse_tx, sse_rx) = mpsc::unbounded_channel();
        app.sse_tx = Some(sse_tx);
        app.sse_rx = Some(sse_rx);
        app.sse_task = Some(tokio::spawn(std::future::pending()));

        app.handle_session_sse_event(SessionSseEvent::TransportFailed {
            session_id: "sess-transport".to_string(),
            stream_epoch: app.sse_epoch,
            message: "network gone".to_string(),
        })
        .unwrap();
        app.handle_chat_key(key(KeyCode::Enter)).await.unwrap();

        assert!(app.chat.streaming);
        assert!(app.stream_disconnected);
        assert_eq!(app.chat.current_response, "partial 你好");
        assert_eq!(app.chat.textarea.lines(), &["new"]);
        assert!(app.chat.messages.is_empty());
    }

    #[tokio::test]
    async fn stale_transport_failure_cannot_detach_a_new_stream_generation() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.sse_epoch = 8;
        app.sse_task = Some(tokio::spawn(std::future::pending()));

        app.handle_session_sse_event(SessionSseEvent::TransportFailed {
            session_id: "sess-1".to_string(),
            stream_epoch: 7,
            message: "old stream failed".to_string(),
        })
        .unwrap();

        assert!(app.sse_task.is_some());
        assert_eq!(app.sse_epoch, 8);
        app.detach_stream();
    }

    #[tokio::test]
    async fn clarification_boundary_complete_preserves_modal_and_draft() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.chat.streaming = true;
        app.handle_sse_event(AgentEvent::NeedClarification {
            question: "Choose carefully".to_string(),
            options: Some(vec!["A".to_string()]),
            tool_call_id: Some("call-1".to_string()),
            tool_name: Some("ConclusionWithOptions".to_string()),
            allow_custom: true,
            source: Some("pause_tool".to_string()),
        })
        .unwrap();
        app.pending_question.as_mut().unwrap().custom = Some("草稿🙂".to_string());
        let epoch = app.answer_epoch;

        app.handle_sse_event(AgentEvent::Complete {
            usage: TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            },
        })
        .unwrap();

        let question = app.pending_question.as_ref().expect("modal remains");
        assert_eq!(question.tool_call_id.as_deref(), Some("call-1"));
        assert_eq!(question.custom.as_deref(), Some("草稿🙂"));
        assert_eq!(app.answer_epoch, epoch);
        assert!(app.chat.streaming);
        app.detach_stream();
    }

    #[test]
    fn successor_complete_without_replayed_started_is_terminal_while_answer_submits() {
        let mut app = app_with_question(vec!["A"]);
        app.pending_question.as_mut().unwrap().submitting = true;
        app.handle_sse_event(AgentEvent::Token {
            content: "恢复完成🙂".to_string(),
        })
        .unwrap();
        app.handle_sse_event(AgentEvent::Complete {
            usage: TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            },
        })
        .unwrap();

        assert!(app.pending_question.is_none());
        assert!(!app.chat.streaming);
        assert_eq!(app.chat.messages.last().unwrap().content, "恢复完成🙂");
    }

    #[tokio::test]
    async fn non_resuming_answer_status_is_not_overwritten_by_ready() {
        let mut app = app_with_question(vec!["A"]);
        let identity = question_identity(&app);
        let epoch = app.answer_epoch;
        app.handle_event(AppEvent::AnswerSubmitted {
            epoch,
            identity,
            answer: "A".to_string(),
            result: Ok("error: session not found".to_string()),
        })
        .await
        .unwrap();

        assert_eq!(app.status_message, "Answered: A (error: session not found)");
    }

    #[tokio::test]
    async fn fresh_execute_waits_for_sse_subscription_handshake() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (execute_seen_tx, execute_seen_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut sse_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut sse_socket).await,
                "/api/v1/events/sess-handshake"
            );

            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "execute must not reach the server before SSE responds"
            );
            sse_socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            sse_socket.flush().await.unwrap();

            let (mut execute_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut execute_socket).await,
                "/api/v1/execute/sess-handshake"
            );
            execute_seen_tx.send(()).ok();
            respond_test_http(
                &mut execute_socket,
                "application/json",
                r#"{"session_id":"sess-handshake","status":"started","events_url":"/api/v1/events/sess-handshake"}"#,
            )
            .await;
        });

        let mut app = App::new(BambooClient::new(&base_url));
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(event_tx);
        app.chat.session_id = Some("sess-handshake".to_string());
        app.start_stream_and_execute("sess-handshake".to_string());

        tokio::time::timeout(std::time::Duration::from_secs(5), execute_seen_rx)
            .await
            .expect("execute should follow the SSE handshake")
            .unwrap();
        server.await.unwrap();
        app.detach_stream();
    }

    #[tokio::test]
    async fn answer_replaces_live_old_stream_before_fast_successor_events() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let (successor_sent_tx, successor_sent_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut successor_sse, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut successor_sse).await,
                "/api/v1/events/sess-1"
            );
            successor_sse
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();
            successor_sse.flush().await.unwrap();

            let (mut respond_socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut respond_socket).await,
                "/api/v1/respond/sess-1"
            );

            // The successor completes before the respond HTTP body is
            // returned. Only a subscriber installed before POST can observe
            // this complete sequence.
            successor_sse
                .write_all(
                    concat!(
                        "data: {\"type\":\"execution_started\",\"run_id\":\"run-successor\",\"session_id\":\"sess-1\",\"started_at\":\"2026-08-10T00:00:00Z\"}\n\n",
                        "data: {\"type\":\"token\",\"content\":\"快速恢复🙂\"}\n\n",
                        "data: {\"type\":\"complete\",\"run_id\":\"run-successor\",\"session_id\":\"sess-1\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            successor_sse.flush().await.unwrap();
            successor_sent_tx.send(()).ok();
            respond_test_http(
                &mut respond_socket,
                "application/json",
                r#"{"auto_resume_status":"started"}"#,
            )
            .await;
        });

        let mut app = app_with_question(vec!["A"]);
        app.client = BambooClient::new(&base_url);
        let (app_tx, _app_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(app_tx);

        // Model the dangerous old state: readiness is still true and the task
        // is still live, but its pause Complete is already queued for the UI.
        let old_epoch = app.sse_epoch;
        let (old_tx, old_rx) = mpsc::unbounded_channel();
        old_tx
            .send(SessionSseEvent::Event {
                session_id: "sess-1".to_string(),
                stream_epoch: old_epoch,
                event: AgentEvent::Complete {
                    usage: TokenUsage {
                        prompt_tokens: 1,
                        completion_tokens: 1,
                        total_tokens: 2,
                    },
                },
            })
            .unwrap();
        app.sse_tx = Some(old_tx);
        app.sse_rx = Some(old_rx);
        app.sse_task = Some(tokio::spawn(std::future::pending()));
        let (_old_ready_tx, old_ready_rx) = tokio::sync::watch::channel(true);
        app.sse_ready = Some(old_ready_rx);

        app.submit_answer("A".to_string());
        assert_ne!(
            app.sse_epoch, old_epoch,
            "answer must replace old generation"
        );
        successor_sent_rx.await.unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while app
            .chat
            .messages
            .last()
            .is_none_or(|message| message.content != "快速恢复🙂")
        {
            assert!(tokio::time::Instant::now() < deadline);
            app.poll_sse();
            tokio::task::yield_now().await;
        }

        assert!(app.pending_question.is_none());
        assert!(!app.chat.streaming);
        assert_eq!(app.chat.messages.last().unwrap().content, "快速恢复🙂");
        server.await.unwrap();
        app.detach_stream();
    }

    #[tokio::test]
    async fn reconnect_recovers_a_clarification_created_during_the_sse_gap() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let fake_server = tokio::spawn(async move {
            let mut event_connections = 0;
            let mut pending_requests = 0;
            while event_connections < 2 || pending_requests < 2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let path = read_test_http_path(&mut stream).await;
                if path == "/api/v1/events/sess-gap" {
                    event_connections += 1;
                    if event_connections == 1 {
                        // Drop the first stream. The authoritative question is
                        // created during this gap and is deliberately absent
                        // from critical-event replay.
                        respond_test_http(&mut stream, "text/event-stream", "").await;
                    } else {
                        respond_test_http(
                            &mut stream,
                            "text/event-stream",
                            "data: {\"type\":\"complete\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
                        )
                        .await;
                    }
                } else if path == "/api/v1/respond/sess-gap/pending" {
                    pending_requests += 1;
                    if pending_requests == 1 {
                        respond_test_http(
                            &mut stream,
                            "application/json",
                            r#"{"has_pending_question":false}"#,
                        )
                        .await;
                    } else {
                        respond_test_http(
                            &mut stream,
                            "application/json",
                            r#"{"has_pending_question":true,"question":"Recovered gap question","options":["Approve"],"allow_custom":false,"tool_call_id":"gap-tool","tool_name":"request_permissions","source":"pause_tool"}"#,
                        )
                        .await;
                    }
                } else {
                    panic!("unexpected fake-server path: {path}");
                }
            }
        });

        let mut app = App::new(BambooClient::new(&base_url));
        app.chat.session_id = Some("sess-gap".to_string());
        let (app_tx, mut app_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(app_tx);
        let (sse_tx, mut sse_rx) = mpsc::unbounded_channel();
        let (sse_task, _ready) = SseStream::start(&base_url, "sess-gap", 0, sse_tx).unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let message = sse_rx.recv().await.expect("SSE control event");
                let reconnected = matches!(
                    &message,
                    SessionSseEvent::Connected {
                        reconnecting: true,
                        ..
                    }
                );
                app.handle_session_sse_event(message).unwrap();
                if reconnected {
                    break;
                }
            }
        })
        .await
        .expect("SSE should reconnect after the deliberate gap");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while app.pending_question.is_none() {
                let reconciled = app_rx
                    .recv()
                    .await
                    .expect("app event channel should stay open");
                assert!(matches!(
                    &reconciled,
                    AppEvent::PendingQuestionReconciled { session_id, .. }
                        if session_id == "sess-gap"
                ));
                app.handle_event(reconciled).await.unwrap();
            }
        })
        .await
        .expect("reconnect reconciliation should recover the question");

        let question = app
            .pending_question
            .as_ref()
            .expect("reconnect should recover the missed clarification");
        assert_eq!(question.question, "Recovered gap question");
        assert_eq!(question.tool_call_id.as_deref(), Some("gap-tool"));

        tokio::time::timeout(std::time::Duration::from_secs(5), fake_server)
            .await
            .expect("fake server should serve every request")
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), sse_task)
            .await
            .expect("terminal replay should stop the SSE task")
            .unwrap();
    }

    #[tokio::test]
    async fn older_reconciliation_response_cannot_supersede_newer_request() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-race".to_string());
        app.pending_reconcile_epoch = 2;
        let answer_epoch = app.answer_epoch;

        // Request 1 finishes after request 2 has already been issued. It must
        // be discarded before it can bump answer_epoch and thereby invalidate
        // request 2's authoritative response.
        app.handle_event(AppEvent::PendingQuestionReconciled {
            session_id: "sess-race".to_string(),
            epoch: answer_epoch,
            reconcile_epoch: 1,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "Stale question A".to_string(),
                options: Some(vec!["A".to_string()]),
                allow_custom: false,
                tool_call_id: Some("tool-a".to_string()),
                tool_name: Some("ConclusionWithOptions".to_string()),
                source: Some("pause_tool".to_string()),
            }),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        assert_eq!(app.answer_epoch, answer_epoch);

        app.handle_event(AppEvent::PendingQuestionReconciled {
            session_id: "sess-race".to_string(),
            epoch: answer_epoch,
            reconcile_epoch: 2,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "Current question B".to_string(),
                options: Some(vec!["B".to_string()]),
                allow_custom: false,
                tool_call_id: Some("tool-b".to_string()),
                tool_name: Some("ConclusionWithOptions".to_string()),
                source: Some("pause_tool".to_string()),
            }),
        })
        .await
        .unwrap();

        let question = app.pending_question.as_ref().expect("newest result wins");
        assert_eq!(question.question, "Current question B");
        assert_eq!(question.tool_call_id.as_deref(), Some("tool-b"));
    }

    #[tokio::test]
    async fn initial_sse_handshake_recovers_a_question_created_before_subscription() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let fake_server = tokio::spawn(async move {
            let mut events_served = false;
            let mut pending_served = false;
            while !events_served || !pending_served {
                let (mut stream, _) = listener.accept().await.unwrap();
                match read_test_http_path(&mut stream).await.as_str() {
                    "/api/v1/events/sess-initial-gap" => {
                        events_served = true;
                        // No NeedClarification replay: the question existed
                        // before this first subscription was established.
                        respond_test_http(
                            &mut stream,
                            "text/event-stream",
                            "data: {\"type\":\"complete\",\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
                        )
                        .await;
                    }
                    "/api/v1/respond/sess-initial-gap/pending" => {
                        pending_served = true;
                        respond_test_http(
                            &mut stream,
                            "application/json",
                            r#"{"has_pending_question":true,"question":"Initial gap question","options":["Continue"],"allow_custom":false,"tool_call_id":"initial-gap-tool","tool_name":"request_permissions","source":"pause_tool"}"#,
                        )
                        .await;
                    }
                    path => panic!("unexpected fake-server path: {path}"),
                }
            }
        });

        let mut app = App::new(BambooClient::new(&base_url));
        app.chat.session_id = Some("sess-initial-gap".to_string());
        let (app_tx, mut app_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(app_tx);
        let (sse_tx, mut sse_rx) = mpsc::unbounded_channel();
        let (sse_task, _ready) =
            SseStream::start(&base_url, "sess-initial-gap", 0, sse_tx).unwrap();

        let connected = tokio::time::timeout(std::time::Duration::from_secs(5), sse_rx.recv())
            .await
            .expect("initial SSE handshake should finish")
            .expect("SSE control channel should stay open");
        assert!(matches!(
            &connected,
            SessionSseEvent::Connected {
                reconnecting: false,
                ..
            }
        ));
        app.handle_session_sse_event(connected).unwrap();

        let reconciled = tokio::time::timeout(std::time::Duration::from_secs(5), app_rx.recv())
            .await
            .expect("initial pending reconciliation should finish")
            .expect("app event channel should stay open");
        app.handle_event(reconciled).await.unwrap();
        let question = app
            .pending_question
            .as_ref()
            .expect("initial handshake must recover the pre-subscription question");
        assert_eq!(question.question, "Initial gap question");
        assert_eq!(question.tool_call_id.as_deref(), Some("initial-gap-tool"));

        tokio::time::timeout(std::time::Duration::from_secs(5), fake_server)
            .await
            .expect("fake server should serve initial sync")
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), sse_task)
            .await
            .expect("terminal frame should stop the SSE task")
            .unwrap();
    }

    /// `is_running: true` reattaches the SSE stream and sets `streaming`.
    /// `event_tx` isn't wired in a bare `App::new`, so `attach_stream`'s
    /// `SseStream::start` call still runs (it only spawns a task, no network
    /// yet) — this asserts the flag flip, not a real connection.
    #[tokio::test]
    async fn session_opened_reattaches_when_running() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        app.opening_session_id = Some("s1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(OpenedSession {
                is_running: true,
                ..opened(vec![])
            }),
        })
        .await
        .unwrap();

        assert!(app.chat.streaming, "is_running: true must reattach");
        assert_eq!(app.status_message, "Reattached — streaming");
    }

    /// A truncated resume surfaces "showing last N of M" as an Info
    /// notification.
    #[tokio::test]
    async fn session_opened_truncated_notifies() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        app.opening_session_id = Some("s1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(OpenedSession {
                truncated: true,
                total_message_count: 5000,
                ..opened(vec![asst_msg("a"), asst_msg("b")])
            }),
        })
        .await
        .unwrap();

        let last = app.notifications.last().expect("truncation notified");
        assert!(last.text.contains("Showing last 2 of 5000 messages"));
    }

    /// A resumed session with a pending question opens the modal, matching
    /// the SSE `NeedClarification` free-text-when-no-options rule.
    #[tokio::test]
    async fn session_opened_with_pending_question_opens_modal() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));

        app.opening_session_id = Some("s1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Ok(OpenedSession {
                pending: Some(PendingQuestion {
                    has_pending_question: true,
                    question: "Proceed?".to_string(),
                    options: None,
                    allow_custom: true,
                    ..Default::default()
                }),
                ..opened(vec![])
            }),
        })
        .await
        .unwrap();

        let q = app.pending_question.as_ref().expect("modal opened");
        assert_eq!(q.question, "Proceed?");
        assert!(q.options.is_empty());
        assert!(q.custom.is_some(), "no options ⇒ free-text entry");
    }

    /// A failed resume surfaces an error and touches nothing else.
    #[tokio::test]
    async fn session_opened_failure_notifies_and_leaves_chat_untouched() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("old".to_string());
        app.chat.textarea.input(key(KeyCode::Char('d')));
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(event_tx);

        app.opening_session_id = Some("s1".to_string());
        app.handle_event(AppEvent::SessionOpened {
            session_id: "s1".to_string(),
            result: Err("not found".to_string()),
        })
        .await
        .unwrap();

        assert_eq!(
            app.chat.session_id.as_deref(),
            Some("old"),
            "a failed resume must not clobber the current session"
        );
        assert!(
            app.opening_session_id.is_none(),
            "the failed resume must release the send gate"
        );
        let last = app.notifications.last().expect("failure notified");
        assert!(last.text.contains("not found"));
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(app.status_message.contains("Failed to open session"));

        app.handle_chat_key(key(KeyCode::Enter)).await.unwrap();
        assert!(app.chat.streaming, "the retained draft can now be sent");
        assert_eq!(app.chat.messages.last().unwrap().content, "d");
        assert!(app.chat.textarea.lines().join("\n").is_empty());
    }

    #[tokio::test]
    async fn pending_question_detail_failure_aborts_session_open() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut history, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut history).await,
                "/api/v1/history/s1"
            );
            respond_test_http(
                &mut history,
                "application/json",
                r#"{"session_id":"s1","messages":[],"truncated":false,"total_message_count":0}"#,
            )
            .await;

            let (mut summary, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut summary).await,
                "/api/v1/sessions/s1"
            );
            respond_test_http(
                &mut summary,
                "application/json",
                r#"{"session":{"id":"s1","model":"model","is_running":false,"has_pending_question":true}}"#,
            )
            .await;

            let (mut pending, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_test_http_path(&mut pending).await,
                "/api/v1/respond/s1/pending"
            );
            let body = r#"{"error":"storage unavailable"}"#;
            let response = format!(
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            pending.write_all(response.as_bytes()).await.unwrap();
            pending.shutdown().await.unwrap();
        });

        let mut app = App::new(BambooClient::new(&base_url));
        app.chat.session_id = Some("old".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);
        app.resume_session("s1".to_string());

        let event = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("resume result should arrive")
            .expect("event channel should stay open");
        assert!(matches!(
            &event,
            AppEvent::SessionOpened {
                session_id,
                result: Err(error),
            } if session_id == "s1" && error.contains("pending question")
        ));
        app.handle_event(event).await.unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("old"));
        assert!(app.pending_question.is_none());
        assert!(app
            .notifications
            .last()
            .is_some_and(|notice| notice.text.contains("pending question")));
        server.await.unwrap();
    }

    /// `Ctrl+N` clears every session-scoped field but keeps the model and
    /// stable Project membership.
    #[tokio::test]
    async fn ctrl_n_clears_session_but_keeps_model() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("s1".to_string());
        app.chat.model = "claude-sonnet-5".to_string();
        app.chat.provider = Some("anthropic".to_string());
        app.chat.project_id = Some("project-tui".to_string());
        app.chat.messages = vec![asst_msg("leftover")];
        app.chat.current_response = "partial".to_string();
        app.chat.token_usage = Some(TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
        });
        app.dismissed_question = Some(active_question(
            "s1",
            "q".to_string(),
            None,
            "tool-1",
            true,
            "",
        ));

        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        assert!(app.chat.session_id.is_none());
        assert!(app.chat.messages.is_empty());
        assert!(app.chat.current_response.is_empty());
        assert!(app.chat.token_usage.is_none());
        assert!(app.pending_question.is_none());
        assert!(
            app.dismissed_question.is_none(),
            "a stale cached question from the old session must not survive"
        );
        assert_eq!(
            app.chat.model, "claude-sonnet-5",
            "model must survive a new session"
        );
        assert_eq!(app.chat.provider.as_deref(), Some("anthropic"));
        assert_eq!(
            app.chat.project_id.as_deref(),
            Some("project-tui"),
            "Project membership must survive a new root session"
        );
        assert_eq!(app.status_message, "New session");
    }

    /// `Ctrl+N` is a no-op while a run is streaming (must stop it explicitly
    /// first, same as every other destructive Sessions/Chat action).
    #[tokio::test]
    async fn ctrl_n_is_noop_while_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.streaming = true;
        app.chat.session_id = Some("s1".to_string());

        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("s1"));
    }

    /// Esc dismisses the question modal but caches it; `Ctrl+Q` brings it
    /// straight back without a network round-trip.
    #[tokio::test]
    async fn esc_then_ctrl_q_restores_the_dismissed_question() {
        let mut app = app_with_question(vec!["Approve", "Deny"]);
        app.chat.session_id = Some("s1".to_string());

        app.handle_question_key(key(KeyCode::Esc)).await.unwrap();
        assert!(app.pending_question.is_none());
        assert!(app.dismissed_question.is_some());
        assert!(app.status_message.contains("Ctrl+Q"));

        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        let q = app.pending_question.as_ref().expect("question restored");
        assert_eq!(q.question, "Run this command?");
        assert_eq!(q.options, vec!["Approve".to_string(), "Deny".to_string()]);
        assert!(app.dismissed_question.is_none(), "cache consumed on reopen");
    }

    /// With nothing cached and no active session, `Ctrl+Q` just notifies
    /// instead of spawning a doomed fetch.
    #[tokio::test]
    async fn ctrl_q_with_nothing_cached_and_no_session_notifies() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.pending_question.is_none());
        let last = app.notifications.last().expect("notified");
        assert!(last.text.contains("No pending question"));
    }

    /// `PendingQuestionChecked` (Ctrl+Q's server round-trip when nothing was
    /// cached) opens the modal when the server reports one waiting.
    #[tokio::test]
    async fn pending_question_checked_opens_when_present() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.handle_event(AppEvent::PendingQuestionChecked {
            session_id: "sess-1".to_string(),
            epoch: app.answer_epoch,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "Still there?".to_string(),
                options: Some(vec!["Yes".to_string()]),
                allow_custom: false,
                tool_call_id: Some("tool-1".to_string()),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        assert_eq!(
            app.pending_question.as_ref().map(|q| q.question.as_str()),
            Some("Still there?")
        );
        assert!(app.chat.streaming);
        assert!(app.sse_task.is_some());
    }

    #[tokio::test]
    async fn ctrl_c_after_server_question_recovery_stops_instead_of_quitting() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.handle_event(AppEvent::PendingQuestionChecked {
            session_id: "sess-1".to_string(),
            epoch: app.answer_epoch,
            result: Ok(PendingQuestion {
                has_pending_question: true,
                question: "Still there?".to_string(),
                options: Some(vec!["Yes".to_string()]),
                allow_custom: false,
                tool_call_id: Some("tool-1".to_string()),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        app.event_tx = Some(event_tx);

        app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        assert!(app.running, "Ctrl+C must not quit while the run is paused");
        assert_eq!(app.status_message, "Stopping...");
    }

    /// ...and reports there's nothing to reopen when the server agrees.
    #[tokio::test]
    async fn pending_question_checked_notifies_when_absent() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.handle_event(AppEvent::PendingQuestionChecked {
            session_id: "sess-1".to_string(),
            epoch: app.answer_epoch,
            result: Ok(PendingQuestion {
                has_pending_question: false,
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        assert!(app.pending_question.is_none());
        let last = app.notifications.last().expect("notified");
        assert!(last.text.contains("No pending question"));
    }

    /// Regression (mirrors `schedule_form_captures_keys_through_handle_key`):
    /// on the Chat tab, idle, a digit must type into the message textarea —
    /// not switch tabs — or something like "top 3 issues" silently jumps to
    /// the Mcp tab on the '3'.
    #[tokio::test]
    async fn digit_on_chat_tab_types_into_textarea_not_tab_switch() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.handle_key(k(KeyCode::Char('3'))).await.unwrap();
        assert_eq!(
            app.tab,
            Tab::Chat,
            "digit must not switch tabs while composing on Chat"
        );
        assert_eq!(app.chat.textarea.lines().join("\n"), "3");
    }

    /// ...but on every other tab digits still switch tabs (unchanged).
    #[tokio::test]
    async fn digit_on_sessions_tab_still_switches_tab() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = |c| KeyEvent::new(c, KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.handle_key(k(KeyCode::Char('3'))).await.unwrap();
        assert_eq!(
            app.tab,
            Tab::Mcp,
            "digit '3' switches to tab index 2 (Mcp) outside Chat"
        );
    }

    /// Alt+Enter inserts a newline into the compose textarea; it must not
    /// send the message the way plain Enter does.
    #[tokio::test]
    async fn alt_enter_inserts_newline_without_sending() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;

        app.handle_key(KeyEvent::new(KeyCode::Char('h'), KeyModifiers::empty()))
            .await
            .unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::empty()))
            .await
            .unwrap();
        assert_eq!(app.chat.textarea.lines().len(), 1);

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT))
            .await
            .unwrap();
        assert_eq!(
            app.chat.textarea.lines().len(),
            2,
            "Alt+Enter must grow the textarea by a line, not send"
        );
        assert_eq!(app.chat.textarea.lines()[0], "hi");
        assert!(!app.chat.streaming, "Alt+Enter must not send the message");
        assert!(app.chat.messages.is_empty());
    }

    /// `?` is reachable as help everywhere except Chat, where it must type
    /// into the textarea instead (F1 is the Chat-safe way to reach help).
    #[tokio::test]
    async fn question_mark_opens_help_except_on_chat() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let k = || KeyEvent::new(KeyCode::Char('?'), KeyModifiers::empty());

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.handle_key(k()).await.unwrap();
        assert!(app.help_visible, "'?' opens help on non-Chat tabs");

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.handle_key(k()).await.unwrap();
        assert!(
            !app.help_visible,
            "'?' must type into the chat textarea, not open help"
        );
        assert_eq!(app.chat.textarea.lines().join("\n"), "?");
    }

    /// F1 opens help on every tab, including Chat, unlike `?`.
    #[tokio::test]
    async fn f1_opens_help_on_chat_tab() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.handle_key(KeyEvent::new(KeyCode::F(1), KeyModifiers::empty()))
            .await
            .unwrap();
        assert!(app.help_visible);
        assert!(
            app.chat.textarea.lines().join("\n").is_empty(),
            "F1 must not leak into the textarea"
        );
    }

    /// Mouse wheel on a list tab (Sessions here) moves the selection instead
    /// of being a dead input, 3 rows per notch and clamped to the list.
    #[test]
    fn mouse_wheel_on_sessions_moves_selection() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.sessions.sessions = vec![
            bare_session("s1"),
            bare_session("s2"),
            bare_session("s3"),
            bare_session("s4"),
            bare_session("s5"),
        ];
        app.sessions.selected = 0;
        let ev = |k| MouseEvent {
            kind: k,
            column: 0,
            row: 0,
            modifiers: crossterm::event::KeyModifiers::empty(),
        };

        app.handle_mouse(ev(MouseEventKind::ScrollDown));
        assert_eq!(app.sessions.selected, 3, "3 rows per notch");
        app.handle_mouse(ev(MouseEventKind::ScrollDown));
        assert_eq!(app.sessions.selected, 4, "clamped to the last index");
        app.handle_mouse(ev(MouseEventKind::ScrollUp));
        assert_eq!(app.sessions.selected, 1);
    }

    // ── Contextual searchable pickers (#636) ──

    fn contextual_session_picker(sessions: Vec<SessionSummary>) -> SessionPicker {
        SessionPicker {
            epoch: 7,
            visible: (0..sessions.len()).collect(),
            sessions,
            query: String::new(),
            selected: 0,
            selection_touched: false,
            loading: false,
            error: None,
            total: 0,
            page_limit: 2,
            next_offset: None,
            mode: SessionPickerMode::Browse,
        }
    }

    #[tokio::test]
    async fn ctrl_p_opens_contextual_picker_and_escape_preserves_chat_draft() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.textarea.input(key(KeyCode::Char('d')));
        app.chat.textarea.input(key(KeyCode::Char('r')));
        app.chat.scroll_offset = 11;
        app.status_message = "Keep this exact status".to_string();
        app.tab = Tab::Sessions;
        app.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.session_picker.is_none(), "Ctrl+P is Chat-tab only");

        app.tab = Tab::Chat;
        app.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.session_picker.is_some());
        app.handle_session_picker_key(key(KeyCode::Esc))
            .await
            .unwrap();
        assert!(app.session_picker.is_none());
        assert_eq!(app.chat.textarea.lines().join("\n"), "dr");
        assert_eq!(app.chat.scroll_offset, 11);
        assert_eq!(app.status_message, "Keep this exact status");
    }

    #[tokio::test]
    async fn ctrl_p_is_ignored_while_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.streaming = true;
        app.handle_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.session_picker.is_none());
    }

    #[tokio::test]
    async fn session_picker_filters_title_model_id_and_status() {
        let mut title_match = bare_session("alpha-12345678");
        title_match.title = "Release investigation".to_string();
        title_match.model = "claude-sonnet".to_string();
        let mut model_match = bare_session("beta-87654321");
        model_match.title = "Other".to_string();
        model_match.model = "gpt-5.6-sol".to_string();
        model_match.is_running = true;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![title_match, model_match]));
        for character in "gpt".chars() {
            app.handle_session_picker_key(key(KeyCode::Char(character)))
                .await
                .unwrap();
        }
        let picker = app.session_picker.as_ref().unwrap();
        assert_eq!(picker.visible.len(), 1);
        assert_eq!(picker.selected_session().unwrap().id, "beta-87654321");

        for _ in 0..3 {
            app.handle_session_picker_key(key(KeyCode::Backspace))
                .await
                .unwrap();
        }
        for character in "running".chars() {
            app.handle_session_picker_key(key(KeyCode::Char(character)))
                .await
                .unwrap();
        }
        assert_eq!(
            app.session_picker
                .as_ref()
                .unwrap()
                .selected_session()
                .unwrap()
                .id,
            "beta-87654321"
        );
    }

    #[tokio::test]
    async fn session_picker_pages_append_deduplicate_and_drop_stale_epochs() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![]));
        app.handle_event(AppEvent::SessionPickerPageLoaded {
            epoch: 7,
            offset: 0,
            result: Ok(ListSessionsEnvelope {
                sessions: vec![bare_session("s1"), bare_session("s2")],
                total: 3,
                limit: 2,
                offset: 0,
                next_offset: Some(2),
            }),
        })
        .await
        .unwrap();
        app.handle_event(AppEvent::SessionPickerPageLoaded {
            epoch: 7,
            offset: 2,
            result: Ok(ListSessionsEnvelope {
                sessions: vec![bare_session("s2"), bare_session("s3")],
                total: 3,
                limit: 2,
                offset: 2,
                next_offset: None,
            }),
        })
        .await
        .unwrap();
        app.handle_event(AppEvent::SessionPickerPageLoaded {
            epoch: 6,
            offset: 0,
            result: Ok(ListSessionsEnvelope {
                sessions: vec![bare_session("stale")],
                total: 1,
                limit: 2,
                offset: 0,
                next_offset: None,
            }),
        })
        .await
        .unwrap();

        let picker = app.session_picker.as_ref().unwrap();
        assert_eq!(
            picker
                .sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s1", "s2", "s3"]
        );
        assert_eq!(picker.total, 3);
    }

    #[tokio::test]
    async fn session_picker_page_failure_stops_until_explicit_retry() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let mut picker = contextual_session_picker(vec![bare_session("s1")]);
        picker.query = "match".to_string();
        picker.next_offset = Some(1);
        picker.total = 2;
        app.session_picker = Some(picker);

        app.handle_event(AppEvent::SessionPickerPageLoaded {
            epoch: 7,
            offset: 1,
            result: Err("offline".to_string()),
        })
        .await
        .unwrap();

        let picker = app.session_picker.as_ref().unwrap();
        assert_eq!(picker.next_offset, Some(1), "retry offset stays available");
        assert!(picker.error.as_deref().unwrap().contains("offline"));
        assert!(
            app.session_picker_task.is_none(),
            "an error must not immediately spawn the same page again"
        );
    }

    #[tokio::test]
    async fn active_session_becomes_selected_when_a_later_page_arrives() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("active".to_string());
        app.session_picker = Some(contextual_session_picker(vec![]));
        for (offset, sessions, next_offset) in [
            (0, vec![bare_session("s1")], Some(1)),
            (1, vec![bare_session("active")], None),
        ] {
            app.handle_event(AppEvent::SessionPickerPageLoaded {
                epoch: 7,
                offset,
                result: Ok(ListSessionsEnvelope {
                    sessions,
                    total: 2,
                    limit: 1,
                    offset,
                    next_offset,
                }),
            })
            .await
            .unwrap();
        }

        assert_eq!(
            app.session_picker
                .as_ref()
                .unwrap()
                .selected_session()
                .unwrap()
                .id,
            "active"
        );
    }

    #[tokio::test]
    async fn lazy_page_preserves_operator_selection_after_navigation() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("active".to_string());
        let mut picker =
            contextual_session_picker(vec![bare_session("s1"), bare_session("chosen")]);
        picker.selected = 1;
        picker.selection_touched = true;
        app.session_picker = Some(picker);

        app.handle_event(AppEvent::SessionPickerPageLoaded {
            epoch: 7,
            offset: 2,
            result: Ok(ListSessionsEnvelope {
                sessions: vec![bare_session("active")],
                total: 3,
                limit: 2,
                offset: 2,
                next_offset: None,
            }),
        })
        .await
        .unwrap();

        assert_eq!(
            app.session_picker
                .as_ref()
                .unwrap()
                .selected_session()
                .unwrap()
                .id,
            "chosen"
        );
    }

    #[tokio::test]
    async fn fresh_version_rebases_an_untouched_rename_draft() {
        let mut original = bare_session("s1");
        original.title = "A".to_string();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![original]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "A".to_string(),
            base_title: "A".to_string(),
            draft_dirty: false,
            metadata_version: None,
            loading_version: true,
            submitting: false,
            error: None,
        };
        let mut fresh = bare_session("s1");
        fresh.title = "B".to_string();

        app.handle_event(AppEvent::SessionPickerVersionLoaded {
            epoch: 7,
            session_id: "s1".to_string(),
            intent: SessionPickerIntent::Rename,
            result: Ok(crate::api::VersionedSession {
                summary: fresh,
                metadata_version: 2,
            }),
        })
        .await
        .unwrap();

        let SessionPickerMode::Rename {
            draft,
            base_title,
            metadata_version,
            error,
            ..
        } = &app.session_picker.as_ref().unwrap().mode
        else {
            panic!("rename mode closed unexpectedly");
        };
        assert_eq!(draft, "B");
        assert_eq!(base_title, "B");
        assert_eq!(*metadata_version, Some(2));
        assert!(error.is_none());
    }

    #[tokio::test]
    async fn fresh_changed_title_does_not_authorize_a_dirty_stale_draft() {
        let mut original = bare_session("s1");
        original.title = "A".to_string();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![original]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "my edit".to_string(),
            base_title: "A".to_string(),
            draft_dirty: true,
            metadata_version: None,
            loading_version: true,
            submitting: false,
            error: None,
        };
        let mut fresh = bare_session("s1");
        fresh.title = "B".to_string();

        app.handle_event(AppEvent::SessionPickerVersionLoaded {
            epoch: 7,
            session_id: "s1".to_string(),
            intent: SessionPickerIntent::Rename,
            result: Ok(crate::api::VersionedSession {
                summary: fresh,
                metadata_version: 2,
            }),
        })
        .await
        .unwrap();

        let SessionPickerMode::Rename {
            draft,
            base_title,
            metadata_version,
            error,
            ..
        } = &app.session_picker.as_ref().unwrap().mode
        else {
            panic!("rename mode closed unexpectedly");
        };
        assert_eq!(draft, "my edit");
        assert_eq!(base_title, "B");
        assert!(metadata_version.is_none());
        assert!(error.as_deref().unwrap().contains("Title changed"));
    }

    #[tokio::test]
    async fn rename_conflict_preserves_draft_and_exposes_retry() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "keep my draft".to_string(),
            base_title: "old title".to_string(),
            draft_dirty: true,
            metadata_version: Some(1),
            loading_version: false,
            submitting: true,
            error: None,
        };
        app.handle_event(AppEvent::SessionPickerPatched {
            epoch: 7,
            session_id: "s1".to_string(),
            intent: SessionPickerIntent::Rename,
            result: Err(crate::api::SessionMutationFailure::test_conflict(2)),
        })
        .await
        .unwrap();

        let SessionPickerMode::Rename {
            draft,
            metadata_version,
            submitting,
            error,
            ..
        } = &app.session_picker.as_ref().unwrap().mode
        else {
            panic!("rename mode must remain open");
        };
        assert_eq!(draft, "keep my draft");
        assert!(metadata_version.is_none());
        assert!(!submitting);
        assert!(error.as_deref().unwrap().contains("Version conflict"));
    }

    #[tokio::test]
    async fn successful_pin_updates_row_without_closing_picker() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Pinning {
            session_id: "s1".to_string(),
            target: true,
            loading_version: false,
            submitting: true,
            error: None,
        };
        let mut updated = bare_session("s1");
        updated.pinned = true;
        app.handle_event(AppEvent::SessionPickerPatched {
            epoch: 7,
            session_id: "s1".to_string(),
            intent: SessionPickerIntent::Pin(true),
            result: Ok(crate::api::VersionedSession {
                summary: updated,
                metadata_version: 2,
            }),
        })
        .await
        .unwrap();
        let picker = app.session_picker.as_ref().unwrap();
        assert!(matches!(picker.mode, SessionPickerMode::Browse));
        assert!(picker.sessions[0].pinned);
    }

    #[tokio::test]
    async fn stale_mutation_response_cannot_satisfy_a_reopened_editor() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "new draft".to_string(),
            base_title: "old title".to_string(),
            draft_dirty: true,
            metadata_version: None,
            loading_version: true,
            submitting: false,
            error: None,
        };
        app.picker_epoch = 8;
        app.session_picker.as_mut().unwrap().epoch = 8;

        app.handle_event(AppEvent::SessionPickerVersionLoaded {
            epoch: 7,
            session_id: "s1".to_string(),
            intent: SessionPickerIntent::Rename,
            result: Ok(crate::api::VersionedSession {
                summary: bare_session("s1"),
                metadata_version: 99,
            }),
        })
        .await
        .unwrap();

        let SessionPickerMode::Rename {
            draft,
            metadata_version,
            loading_version,
            ..
        } = &app.session_picker.as_ref().unwrap().mode
        else {
            panic!("rename editor closed unexpectedly");
        };
        assert_eq!(draft, "new draft");
        assert!(metadata_version.is_none());
        assert!(*loading_version);
    }

    #[tokio::test]
    async fn contextual_delete_confirmation_owns_input_then_returns_to_picker() {
        use crossterm::event::{MouseEvent, MouseEventKind};

        let mut session = bare_session("s1");
        session.title = "Delete me".to_string();
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![
            session,
            bare_session("s2"),
            bare_session("s3"),
            bare_session("s4"),
        ]));
        app.handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert_eq!(app.pending_delete.as_ref().unwrap().0, "s1");

        app.handle_mouse(MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::empty(),
        });
        assert_eq!(
            app.session_picker.as_ref().unwrap().selected,
            0,
            "delete confirmation must consume the wheel"
        );

        app.handle_key(key(KeyCode::Char('n'))).await.unwrap();
        assert!(app.pending_delete.is_none());
        assert!(app.session_picker.is_some());
        assert!(app.session_picker.as_ref().unwrap().query.is_empty());
    }

    #[tokio::test]
    async fn late_action_completion_cannot_erase_a_newer_rename_editor() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "keep this draft".to_string(),
            base_title: "old".to_string(),
            draft_dirty: true,
            metadata_version: Some(1),
            loading_version: false,
            submitting: false,
            error: None,
        };

        app.handle_event(AppEvent::ActionDone {
            outcome: Ok("older delete completed".to_string()),
            reload_tab: true,
            session_picker_epoch: Some(7),
        })
        .await
        .unwrap();

        let SessionPickerMode::Rename { draft, .. } = &app.session_picker.as_ref().unwrap().mode
        else {
            panic!("late generic completion erased the editor");
        };
        assert_eq!(draft, "keep this draft");
    }

    #[test]
    fn session_picker_mouse_wheel_moves_filtered_selection() {
        use crossterm::event::{MouseEvent, MouseEventKind};

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(
            (0..6)
                .map(|index| bare_session(&format!("s{index}")))
                .collect(),
        ));
        app.handle_mouse(MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::empty(),
        });
        assert_eq!(app.session_picker.as_ref().unwrap().selected, 3);
    }

    #[test]
    fn session_picker_renders_at_sixty_columns() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut session = bare_session("session-12345678");
        session.title = "很长的 Unicode 会话标题 for narrow terminals".to_string();
        session.model = "claude-sonnet-5".to_string();
        session.pinned = true;
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![session]));

        let backend = TestBackend::new(60, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains("Session picker"));
        assert!(text.contains("Unicode") || text.contains("很长"));
        assert!(text.contains("★"));
        assert!(text.contains("F2 rename"));
        assert!(text.contains("Esc cancel"));
    }

    #[test]
    fn session_rename_renders_only_the_title_input_cursor() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "New title".to_string(),
            base_title: "Old title".to_string(),
            draft_dirty: true,
            metadata_version: Some(1),
            loading_version: false,
            submitting: false,
            error: None,
        };

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::layout::render_session_picker(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(text.contains("Title:"));
        assert!(!text.contains("Search:"));
        assert_eq!(text.matches('▏').count(), 1, "rename has one focused field");
    }

    #[test]
    fn session_rename_saving_hides_cursor_and_disabled_shortcuts() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Rename {
            session_id: "s1".to_string(),
            draft: "New title".to_string(),
            base_title: "Old title".to_string(),
            draft_dirty: true,
            metadata_version: Some(1),
            loading_version: false,
            submitting: true,
            error: None,
        };

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::layout::render_session_picker(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(text.contains("Saving..."));
        assert!(!text.contains('▏'));
        assert!(!text.contains("Enter save"));
        assert!(!text.contains("Ctrl+R"));
        assert!(!text.contains("Esc"));
    }

    #[test]
    fn session_pin_saving_hides_disabled_shortcuts() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.session_picker = Some(contextual_session_picker(vec![bare_session("s1")]));
        app.session_picker.as_mut().unwrap().mode = SessionPickerMode::Pinning {
            session_id: "s1".to_string(),
            target: true,
            loading_version: false,
            submitting: true,
            error: None,
        };

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::layout::render_session_picker(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(text.contains("Pinning selected session..."));
        assert!(text.contains("Saving..."));
        assert!(!text.contains("Ctrl+R"));
        assert!(!text.contains("Esc"));
    }

    // ── Model picker ──

    fn catalog_model(
        provider: &str,
        model: &str,
        display: &str,
        provider_display: &str,
    ) -> CatalogModel {
        CatalogModel {
            reference: CatalogModelRef {
                provider: provider.to_string(),
                model: model.to_string(),
            },
            display_name: display.to_string(),
            provider_display_name: provider_display.to_string(),
        }
    }

    fn model_picker(models: Vec<CatalogModel>) -> ModelPicker {
        ModelPicker {
            epoch: 1,
            visible: (0..models.len()).collect(),
            models,
            query: String::new(),
            selected: 0,
            loading: false,
            applying: false,
            error: None,
        }
    }

    /// `Ctrl+O` opens the picker only on the Chat tab, and only when idle —
    /// same rationale as `Ctrl+N` being a no-op while streaming.
    #[tokio::test]
    async fn ctrl_o_opens_model_picker_on_chat_tab_only() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Sessions;
        app.handle_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.model_picker.is_none(), "Ctrl+O is Chat-tab only");

        app.tab = Tab::Chat;
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
        app.event_tx = Some(event_tx);
        app.handle_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        let picker = app.model_picker.as_ref().expect("Ctrl+O opens on Chat");
        assert!(picker.loading, "opens immediately with loading: true");
        assert!(picker.models.is_empty());
    }

    #[tokio::test]
    async fn ctrl_o_ignored_while_streaming() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.tab = Tab::Chat;
        app.chat.streaming = true;
        app.handle_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
            .await
            .unwrap();
        assert!(app.model_picker.is_none());
    }

    #[tokio::test]
    async fn model_picker_filters_150_grouped_entries_with_stable_selection() {
        let providers = ["alpha-unique", "beta-unique", "gamma-unique"];
        let models = (0..150)
            .map(|index| {
                let provider = providers[index % providers.len()];
                catalog_model(
                    provider,
                    &format!("model-{index}"),
                    &format!("Display {index}"),
                    provider,
                )
            })
            .collect::<Vec<_>>();
        let target_key = ("alpha-unique".to_string(), "model-117".to_string());
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(models));
        let target_index = app
            .model_picker
            .as_ref()
            .unwrap()
            .models
            .iter()
            .position(|model| model_key(model) == target_key)
            .unwrap();
        app.model_picker.as_mut().unwrap().selected = target_index;

        for character in "alpha-unique".chars() {
            app.handle_model_picker_key(key(KeyCode::Char(character)))
                .await
                .unwrap();
        }

        let picker = app.model_picker.as_ref().unwrap();
        assert_eq!(picker.models.len(), 150);
        assert_eq!(picker.visible.len(), 50);
        assert_eq!(model_key(picker.selected_model().unwrap()), target_key);
        assert!(
            picker.visible.windows(2).all(|pair| pair[0] < pair[1]),
            "filter must preserve grouped catalog order"
        );
    }

    #[tokio::test]
    async fn model_patch_failure_preserves_query_selection_and_underlying_chat() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("s1".to_string());
        app.chat.model = "old-model".to_string();
        app.chat.textarea.input(key(KeyCode::Char('d')));
        app.model_picker = Some(model_picker(vec![
            catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI"),
            catalog_model(
                "anthropic",
                "claude-sonnet-5",
                "Claude Sonnet 5",
                "Anthropic",
            ),
        ]));
        {
            let picker = app.model_picker.as_mut().unwrap();
            picker.query = "claude".to_string();
            picker.refresh_filter(None);
            picker.applying = true;
        }
        let selected_key = model_key(app.model_picker.as_ref().unwrap().selected_model().unwrap());

        app.handle_event(AppEvent::ModelPatched {
            epoch: 1,
            session_id: "s1".to_string(),
            model: catalog_model(
                "anthropic",
                "claude-sonnet-5",
                "Claude Sonnet 5",
                "Anthropic",
            ),
            result: Err("offline".to_string()),
        })
        .await
        .unwrap();

        let picker = app.model_picker.as_ref().unwrap();
        assert_eq!(picker.query, "claude");
        assert_eq!(model_key(picker.selected_model().unwrap()), selected_key);
        assert!(!picker.applying);
        assert!(picker.error.as_deref().unwrap().contains("Enter to retry"));
        assert_eq!(app.chat.model, "old-model");
        assert_eq!(app.chat.textarea.lines().join("\n"), "d");
    }

    #[tokio::test]
    async fn session_switch_invalidates_an_in_flight_model_patch() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("session-a".to_string());
        app.chat.model = "model-a".to_string();
        app.chat.provider = Some("provider-a".to_string());
        app.model_picker = Some(model_picker(vec![catalog_model(
            "provider-a",
            "selected-for-a",
            "Selected for A",
            "Provider A",
        )]));
        app.model_picker.as_mut().unwrap().applying = true;
        app.opening_session_id = Some("session-b".to_string());

        app.handle_event(AppEvent::SessionOpened {
            session_id: "session-b".to_string(),
            result: Ok(OpenedSession {
                model: "model-b".to_string(),
                provider: Some("provider-b".to_string()),
                ..opened(vec![])
            }),
        })
        .await
        .unwrap();
        assert!(app.model_picker.is_none());

        app.handle_event(AppEvent::ModelPatched {
            epoch: 1,
            session_id: "session-a".to_string(),
            model: catalog_model(
                "provider-a",
                "selected-for-a",
                "Selected for A",
                "Provider A",
            ),
            result: Ok(()),
        })
        .await
        .unwrap();

        assert_eq!(app.chat.session_id.as_deref(), Some("session-b"));
        assert_eq!(app.chat.model, "model-b");
        assert_eq!(app.chat.provider.as_deref(), Some("provider-b"));
    }

    #[test]
    fn duplicate_model_ids_require_provider_identity_for_current_group() {
        let models = vec![
            catalog_model("provider-a", "shared", "Shared A", "Provider A"),
            catalog_model("provider-b", "shared", "Shared B", "Provider B"),
        ];
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.model = "shared".to_string();
        app.model_picker = Some(model_picker(models));

        assert!(app
            .model_picker
            .as_ref()
            .unwrap()
            .models
            .iter()
            .all(|model| app.model_group_label(model) != "Current"));

        app.chat.provider = Some("provider-b".to_string());
        let labels = app
            .model_picker
            .as_ref()
            .unwrap()
            .models
            .iter()
            .map(|model| app.model_group_label(model))
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["Provider: Provider A", "Current"]);
    }

    #[test]
    fn model_picker_renders_current_recent_and_provider_groups() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let current = catalog_model("openai", "current", "Current Model", "OpenAI");
        let recent = catalog_model("anthropic", "recent", "Recent Model", "Anthropic");
        let provider = catalog_model("local", "other", "Other Model", "Local");
        let mut models = vec![provider, recent.clone(), current];
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.model = "current".to_string();
        app.recent_models.push_back(model_key(&recent));
        let current = current_model_key(&models, app.chat.provider.as_deref(), &app.chat.model);
        sort_catalog_models(&mut models, current.as_ref(), &app.recent_models);
        app.model_picker = Some(model_picker(models));

        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains("Current"));
        assert!(text.contains("Recent"));
        assert!(text.contains("Provider: Local"));
    }

    /// `↑/↓` move the selection (clamped); `Enter` applies the highlighted
    /// model — `chat.model` keeps the plain model id while `chat.provider`
    /// keeps its paired provider, matching the server's separate compatibility
    /// fields without collapsing identity into a synthetic string.
    #[tokio::test]
    async fn model_picker_navigation_and_enter_applies() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(vec![
            catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI"),
            catalog_model(
                "anthropic",
                "claude-sonnet-5",
                "Claude Sonnet 5",
                "Anthropic",
            ),
        ]));

        app.handle_model_picker_key(key(KeyCode::Down))
            .await
            .unwrap();
        assert_eq!(app.model_picker.as_ref().unwrap().selected, 1);
        app.handle_model_picker_key(key(KeyCode::Down))
            .await
            .unwrap();
        assert_eq!(
            app.model_picker.as_ref().unwrap().selected,
            1,
            "clamped at the last index"
        );
        app.handle_model_picker_key(key(KeyCode::Up)).await.unwrap();
        assert_eq!(app.model_picker.as_ref().unwrap().selected, 0);

        app.handle_model_picker_key(key(KeyCode::Down))
            .await
            .unwrap();
        app.handle_model_picker_key(key(KeyCode::Enter))
            .await
            .unwrap();

        assert!(app.model_picker.is_none(), "Enter closes the picker");
        assert_eq!(
            app.chat.model, "claude-sonnet-5",
            "chat.model gets the plain model id, not provider/model"
        );
        assert_eq!(app.chat.provider.as_deref(), Some("anthropic"));
        assert_eq!(app.status_message, "Model: Claude Sonnet 5 (Anthropic)");
    }

    /// `Enter` while the catalog is still loading (empty list) is a no-op —
    /// there's nothing to apply, and the picker stays open.
    #[tokio::test]
    async fn model_picker_enter_while_loading_is_noop() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(vec![]));
        app.model_picker.as_mut().unwrap().loading = true;
        app.handle_model_picker_key(key(KeyCode::Enter))
            .await
            .unwrap();
        assert!(
            app.model_picker.is_some(),
            "no models to apply yet — picker stays open"
        );
    }

    #[tokio::test]
    async fn model_picker_ctrl_r_refreshes_a_nonempty_catalog_with_no_matches() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.picker_epoch = 1;
        app.model_picker = Some(model_picker(vec![catalog_model(
            "openai", "gpt-4.1", "GPT-4.1", "OpenAI",
        )]));
        {
            let picker = app.model_picker.as_mut().unwrap();
            picker.query = "no-such-model".to_string();
            picker.refresh_filter(None);
            assert!(picker.visible.is_empty());
        }

        app.handle_model_picker_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL))
            .await
            .unwrap();

        let picker = app.model_picker.as_ref().unwrap();
        assert_eq!(picker.epoch, 2, "Ctrl+R must dispatch a catalog reload");
        assert!(
            picker
                .error
                .as_deref()
                .is_some_and(|error| error.contains("not attached")),
            "the test has no event loop, proving reload reached load_model_catalog"
        );
    }

    #[test]
    fn model_picker_no_match_refreshing_hides_the_disabled_retry_shortcut() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(vec![catalog_model(
            "openai", "gpt-4.1", "GPT-4.1", "OpenAI",
        )]));
        {
            let picker = app.model_picker.as_mut().unwrap();
            picker.query = "no-such-model".to_string();
            picker.refresh_filter(None);
            picker.loading = true;
        }

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| crate::ui::layout::render_model_picker(frame, &app))
            .unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(text.contains("Refreshing model catalog..."));
        assert!(text.contains("Esc cancel"));
        assert!(!text.contains("Ctrl+R"));
    }

    /// `Esc` closes the picker without touching `chat.model`.
    #[tokio::test]
    async fn model_picker_esc_leaves_model_unchanged() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.model = "old-model".to_string();
        app.chat.textarea.input(key(KeyCode::Char('d')));
        app.chat.scroll_offset = 9;
        app.status_message = "Keep model status".to_string();
        app.model_picker = Some(model_picker(vec![catalog_model(
            "openai", "gpt-4.1", "GPT-4.1", "OpenAI",
        )]));

        app.handle_model_picker_key(key(KeyCode::Esc))
            .await
            .unwrap();

        assert!(app.model_picker.is_none());
        assert_eq!(app.chat.model, "old-model", "Esc must not change the model");
        assert_eq!(app.chat.textarea.lines().join("\n"), "d");
        assert_eq!(app.chat.scroll_offset, 9);
        assert_eq!(app.status_message, "Keep model status");
    }

    /// Catalog failures remain recoverable without losing the query/selection.
    #[tokio::test]
    async fn catalog_loaded_err_stays_open_with_retry() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(vec![]));
        app.model_picker.as_mut().unwrap().loading = true;

        app.handle_event(AppEvent::CatalogLoaded {
            epoch: 1,
            result: Err("connection refused".to_string()),
        })
        .await
        .unwrap();

        let picker = app.model_picker.as_ref().expect("recoverable picker");
        assert!(!picker.loading);
        assert!(picker
            .error
            .as_deref()
            .unwrap()
            .contains("connection refused"));
    }

    /// An empty catalog stays open with an explicit retry action.
    #[tokio::test]
    async fn catalog_loaded_empty_stays_open_with_retry() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(vec![]));
        app.model_picker.as_mut().unwrap().loading = true;

        app.handle_event(AppEvent::CatalogLoaded {
            epoch: 1,
            result: Ok(ProviderCatalog { models: vec![] }),
        })
        .await
        .unwrap();

        let picker = app.model_picker.as_ref().expect("recoverable picker");
        assert!(picker.models.is_empty());
        assert!(picker.error.as_deref().unwrap().contains("No models"));
    }

    /// A catalog fetch that lands after the picker was already dismissed
    /// (`Esc`) must not reopen it.
    #[tokio::test]
    async fn catalog_loaded_dropped_if_picker_already_closed() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        assert!(app.model_picker.is_none());

        app.handle_event(AppEvent::CatalogLoaded {
            epoch: 1,
            result: Ok(ProviderCatalog {
                models: vec![catalog_model("openai", "gpt-4.1", "GPT-4.1", "OpenAI")],
            }),
        })
        .await
        .unwrap();

        assert!(
            app.model_picker.is_none(),
            "a stale fetch must not reopen a closed picker"
        );
    }

    #[test]
    fn model_picker_renders_model_row() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.model_picker = Some(model_picker(vec![catalog_model(
            "openai", "gpt-4.1", "GPT-4.1", "OpenAI",
        )]));

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("GPT-4.1"), "model display name missing");
        assert!(text.contains("OpenAI"), "provider display name missing");
        assert!(text.contains("openai/gpt-4.1"), "provider/model id missing");
        assert!(text.contains("Esc cancel"), "narrow footer was clipped");
    }
}

#[cfg(test)]
mod auto_serve_tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::sync::Mutex;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    // ── is_loopback_url ──

    #[test]
    fn is_loopback_url_table() {
        let cases: &[(&str, bool)] = &[
            ("http://127.0.0.1:9562", true),
            ("http://localhost:9562", true),
            ("http://[::1]:9562", true),
            ("https://127.0.0.1:9562", true),
            ("https://127.0.0.1", true),              // no port
            ("http://127.0.0.1/api/v1/health", true), // path after authority
            ("http://127.5.0.9:9562", true),          // any 127.x.x.x is loopback
            ("http://example.com:9562", false),       // remote host
            ("https://bamboo.example.com", false),    // remote, https, no port
            ("http://192.168.1.20:9562", false),      // LAN host, not loopback
            // Regression: a hostname merely *starting with* "127." is not an
            // IPv4 127.x.x.x literal and must not be treated as loopback.
            ("http://127.0.0.1.evil.example.com:9562", false),
            // Regression: an out-of-range octet is not a valid IPv4 literal
            // at all, even though the string starts with "127.".
            ("http://127.256.0.1:9562", false),
        ];
        for (url, expected) in cases {
            assert_eq!(is_loopback_url(url), *expected, "url={url}");
        }
    }

    // ── discover_bamboo_bin ──

    /// Real files under a fresh, uniquely-named temp dir (no tempfile crate —
    /// no new deps). Parallel-safe: each call gets its own subdirectory.
    fn unique_temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "bamboo-tui-test-{}-{label}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn touch_bin(dir: &Path) -> PathBuf {
        let path = dir.join(bamboo_bin_name());
        std::fs::write(&path, b"").expect("write fake binary");
        path
    }

    #[test]
    fn discover_bamboo_bin_env_override_wins_even_if_missing() {
        let sibling_dir = unique_temp_dir("env-override-sibling");
        touch_bin(&sibling_dir); // a real sibling binary exists...
        let path_dir = unique_temp_dir("env-override-path");
        touch_bin(&path_dir); // ...and a real PATH binary exists too...
        let missing = unique_temp_dir("env-override-missing").join("bamboo-does-not-exist");

        // ...but the env override still wins, even though it doesn't exist.
        let result = discover_bamboo_bin(
            Some(missing.clone()),
            Some(&sibling_dir),
            Some(&path_dir.to_string_lossy()),
        );
        assert_eq!(result, Some(missing));
    }

    #[test]
    fn discover_bamboo_bin_sibling_beats_path() {
        let exe_dir = unique_temp_dir("sibling-wins-exe");
        let sibling = touch_bin(&exe_dir);
        let path_dir = unique_temp_dir("sibling-wins-path");
        touch_bin(&path_dir);

        let result = discover_bamboo_bin(None, Some(&exe_dir), Some(&path_dir.to_string_lossy()));
        assert_eq!(result, Some(sibling));
    }

    #[test]
    fn discover_bamboo_bin_falls_back_to_path() {
        let exe_dir = unique_temp_dir("path-fallback-exe"); // no sibling binary
        let path_dir = unique_temp_dir("path-fallback-path");
        let on_path = touch_bin(&path_dir);

        let result = discover_bamboo_bin(None, Some(&exe_dir), Some(&path_dir.to_string_lossy()));
        assert_eq!(result, Some(on_path));
    }

    #[test]
    fn discover_bamboo_bin_none_when_nothing_found() {
        let exe_dir = unique_temp_dir("none-exe"); // empty
        let path_dir = unique_temp_dir("none-path"); // empty

        let result = discover_bamboo_bin(None, Some(&exe_dir), Some(&path_dir.to_string_lossy()));
        assert_eq!(result, None);
        // No exe_dir/PATH at all either.
        assert_eq!(discover_bamboo_bin(None, None, None), None);
    }

    // ── serve_offer modal key routing ──

    fn app_with_offer() -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.serve_offer = Some(ServeOffer {
            url: "http://127.0.0.1:9562".to_string(),
        });
        app
    }

    #[test]
    fn serve_offer_n_dismisses_without_spawning() {
        let mut app = app_with_offer();
        app.handle_serve_offer_key(key(KeyCode::Char('n')));
        assert!(app.serve_offer.is_none());
        assert!(app.spawned_server.is_none());
        assert!(app.status_message.contains("--auto-serve"));
    }

    #[test]
    fn serve_offer_esc_dismisses_without_spawning() {
        let mut app = app_with_offer();
        app.handle_serve_offer_key(key(KeyCode::Esc));
        assert!(app.serve_offer.is_none());
        assert!(app.spawned_server.is_none());
    }

    /// The offer must swallow every other key too (e.g. digits, which
    /// elsewhere switch tabs) — verified through the full `handle_key` path
    /// so the modal-precedence routing itself (not just the leaf handler) is
    /// exercised.
    #[tokio::test]
    async fn serve_offer_captures_digits_instead_of_switching_tabs() {
        let mut app = app_with_offer();
        let starting_tab = app.tab;

        app.handle_key(key(KeyCode::Char('3'))).await.unwrap();

        assert_eq!(app.tab, starting_tab);
        assert!(app.serve_offer.is_some());
    }

    /// Single crate-wide lock serializing tests that mutate the
    /// process-global `BAMBOO_BIN` env var (the repo's convention for
    /// unavoidable env-manipulating tests — see e.g.
    /// `bamboo-config/src/paths.rs`'s `env_cache_lock_acquire`).
    static BAMBOO_BIN_ENV_LOCK: Mutex<()> = Mutex::new(());

    /// `y` starts the spawn flow; pointing `BAMBOO_BIN` at a path that
    /// doesn't exist exercises the resolve-succeeds-but-`Command::spawn`-
    /// fails path without touching any real process, and must notify an
    /// error and clear the offer rather than leaving it open or panicking.
    #[tokio::test]
    async fn serve_offer_y_with_broken_bamboo_bin_notifies_error_and_clears_offer() {
        let _guard = BAMBOO_BIN_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let original = std::env::var_os("BAMBOO_BIN");
        let missing = unique_temp_dir("broken-bin").join("bamboo-does-not-exist");
        std::env::set_var("BAMBOO_BIN", &missing);

        let mut app = app_with_offer();
        app.handle_serve_offer_key(key(KeyCode::Char('y')));

        match original {
            Some(v) => std::env::set_var("BAMBOO_BIN", v),
            None => std::env::remove_var("BAMBOO_BIN"),
        }

        assert!(app.serve_offer.is_none());
        assert!(app.spawned_server.is_none());
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(last.text.contains("Failed to spawn"));
    }

    // ── AppEvent::LocalServerReady ──

    #[tokio::test]
    async fn local_server_ready_ok_connects_and_resumes_pending_session() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        // Simulates the `--session-id`-at-startup-while-disconnected case:
        // `resume_session` already ran once (in `run`) and failed, leaving
        // `session_id` set but `messages` empty.
        app.chat.session_id = Some("sess-1".to_string());
        assert!(app.chat.messages.is_empty());
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_event(AppEvent::LocalServerReady(Ok(4242)))
            .await
            .unwrap();

        assert!(app.connected);
        // `resume_session` sets this status immediately (its fetch itself is
        // off the event loop) — proof a resume was actually initiated.
        assert_eq!(app.status_message, "Resuming session...");
        assert!(app.notifications.iter().any(|n| n.text.contains("4242")));
    }

    #[tokio::test]
    async fn local_server_ready_ok_does_not_resume_when_messages_already_present() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        app.chat.session_id = Some("sess-1".to_string());
        app.chat.messages.push(ChatMessage {
            role: MessageRole::User,
            content: "hi".to_string(),
            tool_calls: Vec::new(),
            reasoning: None,
        });
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_event(AppEvent::LocalServerReady(Ok(1)))
            .await
            .unwrap();

        assert!(app.connected);
        assert_ne!(app.status_message, "Resuming session...");
    }

    #[tokio::test]
    async fn local_server_ready_err_notifies_and_stays_disconnected() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let (tx, _rx) = mpsc::unbounded_channel();
        app.event_tx = Some(tx);

        app.handle_event(AppEvent::LocalServerReady(Err("boom".to_string())))
            .await
            .unwrap();

        assert!(!app.connected);
        assert!(app.spawned_server.is_none());
        let last = app.notifications.last().expect("notified");
        assert_eq!(last.level, NoticeLevel::Error);
        assert!(last.text.contains("boom"));
    }

    /// The offer modal renders through the normal `ui::render` overlay chain
    /// without panicking, and shows the URL it's offering to serve.
    #[test]
    fn serve_offer_renders_without_panicking() {
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let app = app_with_offer();
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| crate::ui::render(f, &app)).unwrap();

        let buf = terminal.backend().buffer().clone();
        let text: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(text.contains("127.0.0.1:9562"), "offer URL missing");
        assert!(text.contains("Start a local"), "offer prompt missing");
    }
}
