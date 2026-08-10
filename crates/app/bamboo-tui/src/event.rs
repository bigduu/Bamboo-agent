use crossterm::event::{KeyEvent, MouseEvent};

use crate::api::types::{
    CatalogModel, ListSessionsEnvelope, McpServer, PendingQuestion, ProviderCatalog, Schedule,
    Skill, SkillDetail, ToolInfo,
};
use crate::api::{RespondFailure, SessionMutationFailure, VersionedSession};
use crate::app::{OpenedSession, QuestionIdentity, SessionPickerIntent};

/// Result of a background API call, delivered back to the event loop so the call
/// never blocks the UI thread. `Err` carries a display string.
type Loaded<T> = Result<T, String>;

pub enum AppEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    /// Terminal resized; the next loop iteration redraws at the new size (the
    /// dimensions themselves aren't needed — ratatui re-measures on draw).
    Resize,

    // ── Non-blocking API results (posted by spawned tasks) ──
    SessionsLoaded(Loaded<ListSessionsEnvelope>),
    McpServersLoaded(Loaded<Vec<McpServer>>),
    McpToolsLoaded(Loaded<Vec<ToolInfo>>),
    SchedulesLoaded(Loaded<Vec<Schedule>>),
    SkillsLoaded(Loaded<Vec<Skill>>),
    ConfigLoaded(Loaded<serde_json::Value>),
    /// A background mutation finished; the outcome is `Ok(success message)` or
    /// `Err(failure message)` so the receiver can classify it (info vs error)
    /// without sniffing the display text. `reload_tab` reloads the current tab.
    ActionDone {
        outcome: Loaded<String>,
        reload_tab: bool,
        /// The contextual Session picker generation that originated this
        /// action, if any. Generic background actions must never reload and
        /// erase a newer picker/editor merely because they finish late.
        session_picker_epoch: Option<u64>,
    },
    /// A session DELETE finished. Unlike generic actions, this retains the
    /// deleted id so Chat state can detach atomically when the operator
    /// deleted the session currently shown beneath either session UI.
    SessionDeleted {
        session_id: String,
        result: Loaded<()>,
        session_picker_epoch: Option<u64>,
    },
    /// A chat turn was created + started; carries the new session id.
    ChatStarted(Loaded<String>),
    /// The `execute` POST that kicks off a run failed (server down, 4xx/5xx).
    /// Since no SSE terminal event will ever arrive for a run that never
    /// started, this is how `chat.streaming` gets unstuck.
    ExecuteFailed(String),
    /// The background `stop` POST finished; `Err` still finalizes streaming
    /// locally (see `stop_streaming`) so the operator regains control even if
    /// the server is unreachable.
    StopFinished(Loaded<()>),
    /// A skill's detail view finished loading (`Enter` on the Skills tab).
    SkillDetailLoaded(Loaded<SkillDetail>),
    /// A session resume (Sessions-tab `Enter` or `--session-id` at startup)
    /// finished fetching history + summary (+ pending question, if any).
    /// Carries `session_id` alongside the result so the handler can still
    /// report which session failed to open.
    SessionOpened {
        session_id: String,
        result: Result<OpenedSession, String>,
    },
    /// `Ctrl+Q` with no cached dismissed question found one on the server (or
    /// confirmed there isn't one). Session + epoch bind the async result to
    /// the context that requested it so a late fetch cannot replace a newer
    /// session/tool-call question.
    PendingQuestionChecked {
        session_id: String,
        epoch: u64,
        result: Loaded<PendingQuestion>,
    },
    /// Authoritative pending-question state fetched after an SSE handshake.
    /// The epoch prevents a late fetch from replacing a question or answer
    /// that changed while the request was in flight.
    PendingQuestionReconciled {
        session_id: String,
        epoch: u64,
        reconcile_epoch: u64,
        result: Loaded<PendingQuestion>,
    },
    /// Server-state reconciliation after a rejected answer whose 400/409
    /// status indicates the question may have changed or been consumed.
    PendingQuestionRefreshed {
        session_id: String,
        epoch: u64,
        identity: QuestionIdentity,
        result: Loaded<PendingQuestion>,
    },
    /// The answer POST for the pending question finished (`submit_answer`
    /// spawns it off the event loop — awaiting it inline froze the whole UI
    /// for the round-trip). `epoch` is the submission epoch captured when the
    /// POST was spawned; the handler discards the event when it no longer
    /// matches `App::answer_epoch` (the question was superseded mid-flight —
    /// new question arrived, session switched, run finalized, modal
    /// reopened). `answer` is echoed back for the post-submit status message;
    /// `result` carries the server's `auto_resume_status` on success.
    AnswerSubmitted {
        epoch: u64,
        identity: QuestionIdentity,
        answer: String,
        result: Result<String, RespondFailure>,
    },
    /// `Ctrl+O`'s provider-catalog fetch finished. `epoch` makes close/reopen
    /// safe when an older HTTP response arrives after the new overlay opened.
    CatalogLoaded {
        epoch: u64,
        result: Loaded<ProviderCatalog>,
    },
    /// Recoverable model PATCH result. The picker stays open until success so
    /// query/selection and the chat draft survive validation/network errors.
    ModelPatched {
        epoch: u64,
        session_id: String,
        model: CatalogModel,
        result: Loaded<()>,
    },
    /// One lazily-loaded page for the contextual session picker. Pages are
    /// requested serially and capped in memory; stale epochs are discarded.
    SessionPickerPageLoaded {
        epoch: u64,
        offset: usize,
        result: Loaded<ListSessionsEnvelope>,
    },
    /// Fresh session summary + ETag loaded before a rename/pin mutation.
    SessionPickerVersionLoaded {
        epoch: u64,
        session_id: String,
        intent: SessionPickerIntent,
        result: Loaded<VersionedSession>,
    },
    /// Optimistic rename/pin PATCH completed. A 412 remains typed so the UI
    /// can preserve the draft and offer an explicit refetch/retry action.
    SessionPickerPatched {
        epoch: u64,
        session_id: String,
        intent: SessionPickerIntent,
        result: Result<VersionedSession, SessionMutationFailure>,
    },
    /// The auto-serve health-poll waiter finished: `Ok(pid)` once
    /// `client.health()` succeeded (carries the spawned server's pid, for the
    /// confirmation notice); `Err(message)` if it never became healthy within
    /// the poll deadline (`message` names the server log path so the operator
    /// can diagnose it). See `App::spawn_local_server`.
    LocalServerReady(Loaded<u32>),
}
