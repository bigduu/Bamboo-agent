//! `bamboo -p` — a COMPLETE bamboo server, headless: boots the full `AppState`
//! (root tool surface incl. SubAgent → can spawn actor children), runs the
//! prompt on a root session through the same execute path the HTTP API uses,
//! streams events to the terminal, and exits when the whole tree is finished
//! (parent + children, including suspend/resume coordination).
//!
//! `-s/--session <id>` continues an existing session's loop — the headless
//! equivalent of sending the next chat message.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use actix_web::web;

use bamboo_agent_core::storage::Storage as _;
use bamboo_agent_core::{Message, Role, Session, SessionKind};
use bamboo_server::app_state::AppState;
use bamboo_server::handlers::agent::execute::{handler as execute_handler, ExecuteRequest};
use bamboo_server::handlers::agent::respond::{submit_response, RespondRequest};
use bamboo_tools::permission::PermissionMode;

/// Parse a `--permission-mode` value into a [`PermissionMode`].
fn parse_permission_mode(s: &str) -> Result<PermissionMode, String> {
    match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "default" | "ask" => Ok(PermissionMode::Default),
        "plan" => Ok(PermissionMode::Plan),
        "accept-edits" | "edits" => Ok(PermissionMode::AcceptEdits),
        "dont-ask" | "deny" => Ok(PermissionMode::DontAsk),
        "bypass" | "bypass-permissions" | "yolo" => Ok(PermissionMode::BypassPermissions),
        other => Err(format!(
            "unknown --permission-mode '{other}' (expected: default | plan | accept-edits | dont-ask | bypass)"
        )),
    }
}

/// Pick the option string matching `needle` (case-insensitive, exact-or-contains)
/// from a pending question's options, falling back to `needle` itself. Used to map
/// a y/n answer to the exact option the respond validator expects.
fn pick_option(options: &[String], needle: &str) -> String {
    options
        .iter()
        .find(|opt| opt.eq_ignore_ascii_case(needle) || opt.to_ascii_lowercase().contains(needle))
        .cloned()
        .unwrap_or_else(|| needle.to_string())
}

/// What to do with a parked pending question in headless mode, decided BEFORE
/// touching stdin so a non-interactive run can never block on un-answerable input.
enum HeadlessPromptAction {
    /// stdin is a terminal — prompt the operator interactively.
    Prompt,
    /// stdin is NOT a terminal — resolve deterministically without reading stdin.
    /// `response` is the answer to submit (the `deny` option, fail-closed);
    /// `message` explains the auto-decision to the operator/log.
    Resolve { response: String, message: String },
    /// stdin is NOT a terminal AND the gate has no submittable fail-closed answer
    /// (no `deny`-labelled option and custom answers are disabled — e.g.
    /// `exit_plan_mode`'s `["Approve …", "Stay in plan mode"]`). Routing the
    /// literal `"deny"` fallback through `submit_response` would be guaranteed
    /// invalid and the run would exit on a `respond rejected` error. Skip that
    /// doomed round-trip and exit gracefully — still fail-closed (the gated tool
    /// never runs), just cleanly (#80). `message` explains why to the operator.
    Abort { message: String },
}

/// Decide how to handle a pending question given whether stdin is a terminal.
///
/// This is the testable seam for issue #80: a background / CI / piped `bamboo -p`
/// inherits an open, non-TTY stdin that never reaches EOF, so the old
/// unconditional `read_line` blocked forever. When `is_terminal` is false we
/// resolve the question deterministically and FAIL CLOSED — we deny the gate
/// rather than auto-approve (denying is the safe default; auto-approving would be
/// a security regression) — and never read stdin. When `is_terminal` is true we
/// fall through to the existing interactive prompt.
fn resolve_headless_permission(
    is_terminal: bool,
    pending: &bamboo_agent_core::PendingQuestion,
) -> HeadlessPromptAction {
    if is_terminal {
        return HeadlessPromptAction::Prompt;
    }
    // Fail closed: pick the question's own `deny` option (e.g. permission gates use
    // ["Approve", "Deny"]) so the respond validator's exact-match accepts it.
    let deny = pick_option(&pending.options, "deny");
    // `pick_option` falls back to the literal "deny" when no option matches. For an
    // Approve-first gate with no deny-labelled option (e.g. `exit_plan_mode`'s
    // `["Approve …", "Stay in plan mode"]`), that literal is NOT one of the options,
    // so — unless the gate accepts custom answers — submitting it is guaranteed to
    // be rejected by the respond validator. Don't route a doomed answer: abort
    // gracefully instead (still fail-closed; the gated tool never runs) (#80).
    let deny_is_submittable =
        pending.allow_custom || pending.options.iter().any(|opt| opt == &deny);
    if !deny_is_submittable {
        return HeadlessPromptAction::Abort {
            message: format!(
                "cannot auto-resolve this permission gate in headless mode \
                 (no deny option among {:?} and custom answers are disabled): \"{}\". \
                 Pass --permission-mode bypass|accept-edits|dont-ask to run non-interactively.",
                pending.options,
                pending.question.trim()
            ),
        };
    }
    HeadlessPromptAction::Resolve {
        response: deny,
        message: format!(
            "no interactive approver (stdin is not a TTY): auto-denying \"{}\". \
             Pass --permission-mode bypass|accept-edits|dont-ask to run non-interactively.",
            pending.question.trim()
        ),
    }
}

/// Prompt the operator on the terminal for a parked pending question (typically a
/// permission gate, since headless has no UI approver) and map the answer to a
/// response string the engine's respond flow understands: `y`/`yes` → `approve`,
/// `n`/`no`/empty → `deny`, a number selects a listed option, anything else is
/// sent verbatim.
///
/// When stdin is NOT a terminal (background, pipe, CI), we do NOT block on a stdin
/// read — issue #80: such stdin can stay open without ever reaching EOF, so a
/// `read_line` would hang forever. Instead we resolve the gate deterministically,
/// FAIL CLOSED (deny), and return that answer. Returns `None` on EOF when stdin
/// IS a terminal (operator closed the input).
async fn prompt_pending_response(pending: &bamboo_agent_core::PendingQuestion) -> Option<String> {
    use std::io::IsTerminal as _;
    use std::io::Write as _;
    use tokio::io::{AsyncBufReadExt, BufReader};

    // Decide up front, before touching stdin, so a non-TTY run can't wedge.
    match resolve_headless_permission(std::io::stdin().is_terminal(), pending) {
        HeadlessPromptAction::Resolve { response, message } => {
            eprintln!("\n⏸  {message}");
            return Some(response);
        }
        HeadlessPromptAction::Abort { message } => {
            // No submittable fail-closed answer: report and leave the question
            // unanswered rather than routing a guaranteed-invalid one (#80).
            eprintln!("\n⏸  {message}");
            return None;
        }
        HeadlessPromptAction::Prompt => {}
    }

    eprintln!("\n⏸  {}", pending.question.trim());
    if !pending.options.is_empty() {
        let opts: Vec<String> = pending
            .options
            .iter()
            .enumerate()
            .map(|(i, o)| format!("[{}] {}", i + 1, o))
            .collect();
        eprintln!("   options: {}", opts.join("   "));
    }
    eprint!("   approve? [y/N] (y=approve · n=deny · or a choice): ");
    let _ = std::io::stderr().flush();

    let mut line = String::new();
    match BufReader::new(tokio::io::stdin())
        .read_line(&mut line)
        .await
    {
        Ok(0) | Err(_) => None,
        Ok(_) => {
            let trimmed = line.trim();
            // The respond validator requires an EXACT match against the pending
            // question's options (unless `allow_custom`), so map y/n to the real
            // option string (e.g. permission gates use ["Approve", "Deny"]) rather
            // than a hardcoded literal.
            let answer = match trimmed.to_ascii_lowercase().as_str() {
                "y" | "yes" => pick_option(&pending.options, "approve"),
                "n" | "no" | "" => pick_option(&pending.options, "deny"),
                _ => trimmed
                    .parse::<usize>()
                    .ok()
                    .and_then(|n| pending.options.get(n.wrapping_sub(1)).cloned())
                    .unwrap_or_else(|| trimmed.to_string()),
            };
            Some(answer)
        }
    }
}

pub struct HeadlessArgs {
    pub prompt: String,
    /// Continue this existing root session instead of creating a new one.
    pub session: Option<String>,
    pub model: Option<String>,
    pub workspace: Option<PathBuf>,
    pub data_dir: PathBuf,
    /// Override the permission mode for this headless run (no interactive
    /// approver exists). One of: `default`, `plan`, `accept-edits`, `dont-ask`,
    /// `bypass`. `None` keeps the configured/default posture (ask-on-high-risk),
    /// which strands tool-gated runs at the first gated tool.
    pub permission_mode: Option<String>,
    /// NDJSON streaming: one JSON object per line on stdout —
    /// `session_started`, every AgentEvent verbatim, then a final `result`
    /// envelope. Nothing else is written to stdout (logs go to stderr), so
    /// the stream is pipe-safe.
    pub stream_json: bool,
    /// Per-run reasoning effort override (`low`/`medium`/`high`/`xhigh`/`max`).
    /// `None` keeps the active provider/config default.
    pub reasoning_effort: Option<String>,
    /// Per-run skill mode (e.g. `code`, `ask`). `None` keeps the session/config
    /// default. Threads into `ExecuteRequest.skill_mode`.
    pub skill_mode: Option<String>,
    /// Per-run provider override (e.g. `anthropic`, `openai`). Combined with a
    /// bare `--model <id>` to form a `provider:model` reference, or used alone to
    /// select that provider (its configured default model). When `--model` is
    /// itself `provider:model`, that provider wins and a conflicting `--provider`
    /// errors. Threads into `ExecuteRequest.model_ref` / `.provider`.
    pub provider: Option<String>,
    /// Cancel the run if it hasn't finished within this many seconds (wall
    /// clock; counts any permission-gate round trips). `None` never times out.
    /// This is CLIENT-side only — `ExecuteRequest` has no per-run deadline
    /// field to delegate to, so this reuses the same cancellation path as
    /// Ctrl-C rather than inventing server-side behavior.
    pub timeout_secs: Option<u64>,
}

/// Validate `--timeout` at the CLI boundary: zero would fire (almost)
/// immediately, which is never useful and is more likely a typo than intent.
fn validate_timeout_secs(secs: Option<u64>) -> Result<(), String> {
    match secs {
        Some(0) => Err("invalid --timeout '0' (expected a positive number of seconds)".to_string()),
        _ => Ok(()),
    }
}

pub async fn run(args: HeadlessArgs) -> Result<(), String> {
    // Validate cheap argument errors BEFORE any state is booted or a session is
    // created/persisted — an invalid `--timeout 0` must not leave an orphaned
    // session on disk.
    validate_timeout_secs(args.timeout_secs)?;

    // Full server assembly — identical to `bamboo serve`, minus the HTTP listener.
    let state = AppState::new(args.data_dir.clone())
        .await
        .map_err(|e| format!("boot app state: {e}"))?;

    // Headless has no interactive approver, so the default ask-on-high-risk
    // posture strands a tool-using run at the first gated tool. An explicit
    // `--permission-mode` lets `bamboo -p` proceed (e.g. `bypass`). The mode rides
    // on the shared PermissionConfig and is read per check, so it applies to the
    // whole run (and any child actors built from the same checker).
    if let Some(raw) = args.permission_mode.as_deref() {
        let mode = parse_permission_mode(raw)?;
        state.permission_checker.set_permission_mode(mode);
        eprintln!("• permission mode: {raw}");
    }

    // ---- session: continue or create ----
    let session_id = match &args.session {
        Some(id) => {
            let existing = state
                .storage
                .load_session(id)
                .await
                .map_err(|e| format!("load session {id}: {e}"))?
                .ok_or_else(|| format!("session '{id}' not found"))?;
            if existing.kind != SessionKind::Root {
                return Err(format!("session '{id}' is not a root session"));
            }
            id.clone()
        }
        None => {
            let mut title: String = args.prompt.chars().take(48).collect();
            if title.len() < args.prompt.len() {
                title.push('…');
            }
            let mut session = Session::new(uuid::Uuid::new_v4().to_string(), String::new());
            session.title = title;
            session.workspace = args
                .workspace
                .clone()
                .or_else(|| std::env::current_dir().ok())
                .map(|w| w.to_string_lossy().into_owned());
            let id = session.id.clone();
            state
                .storage
                .save_session(&session)
                .await
                .map_err(|e| format!("save session: {e}"))?;
            state
                .session_store
                .save_session(&session)
                .await
                .map_err(|e| format!("index session: {e}"))?;
            id
        }
    };

    // Append the prompt as the driving user message (execute runs the session's
    // last user turn — same contract as the HTTP API).
    //
    // #74: the `no_human_approver` posture is no longer set here. It rides on the
    // `ExecuteRequest` below (`no_human_approver: true`) and is re-derived +
    // OVERWRITTEN per execute by the handler, so a session first run headlessly
    // and later reopened interactively correctly resets to the human-present
    // posture instead of staying sticky-true.
    // Resolve `-m`/`--provider` once, BEFORE the session is persisted below. A
    // bare `-m <model>` (no colon, no `--provider`) binds to the configured
    // default provider — the same grammar `actor run` uses. Applies to the
    // initial execute and to any resume.
    let default_provider = state.config.read().await.provider.clone();
    let model_selection = resolve_model_selection(&args.model, &args.provider, &default_provider)?;

    // Resolve the concrete provider+model ref to pin onto the session. For a
    // bare/colon `-m` this is the parsed ref. For `--provider P` ALONE we resolve
    // P's configured default model here so the pin is still concrete: the new
    // cascade's `defaults.chat` fallback is a single GLOBAL ref, so passing only
    // a provider would otherwise be ignored and the run would keep using the
    // global default provider/model. If P has no configured model we leave the
    // ref unset and fall back to the request's `provider` field (the legacy
    // cascade resolves P's provider-aware default, or execute reports no model).
    let session_model_ref: Option<bamboo_domain::ProviderModelRef> = match (
        model_selection.model_ref.clone(),
        model_selection.provider.as_deref(),
    ) {
        (Some(model_ref), _) => Some(model_ref),
        (None, Some(provider)) => {
            let config = state.config.read().await;
            bamboo_engine::model_config_helper::get_default_model_for_provider(&config, provider)
                .ok()
                .map(|model| bamboo_domain::ProviderModelRef::new(provider, model))
        }
        (None, None) => None,
    };

    {
        let mut session = state
            .storage
            .load_session(&session_id)
            .await
            .map_err(|e| format!("load session: {e}"))?
            .ok_or_else(|| "session vanished".to_string())?;
        session.add_message(Message::user(args.prompt.clone()));
        // Pin the chosen model onto the SESSION, not just the request. The server
        // has two model cascades gated on `features.provider_model_ref`; the
        // legacy one (the default) ranks the request's model BELOW the provider's
        // configured default model, so a request-only pin is silently outranked
        // whenever that default is set. `session.model`/`model_ref` is the
        // highest-priority source in BOTH cascades, so persisting it here is what
        // actually makes `-m`/`--provider` win — via the engine's own persist
        // helper (the same call the execute handler makes) so there is no forked
        // model write.
        if let Some(model_ref) = &session_model_ref {
            bamboo_engine::session_app::provider_model::persist_model_ref(&mut session, model_ref);
        }
        state
            .storage
            .save_session(&session)
            .await
            .map_err(|e| format!("save session: {e}"))?;
        let _ = state.session_store.save_session(&session).await;
    }

    // Subscribe to the session's event stream BEFORE starting execution.
    let sender = state.get_session_event_sender(&session_id).await;
    let mut events = sender.subscribe();

    // Parse the optional reasoning-effort override once (applies to the initial
    // execute and to any resume after a pending question).
    let reasoning_effort = match args.reasoning_effort.as_deref() {
        Some(raw) => Some(
            bamboo_domain::reasoning::ReasoningEffort::parse(raw).ok_or_else(|| {
                format!(
                    "invalid --reasoning-effort '{raw}' (expected: low | medium | high | xhigh | max)"
                )
            })?,
        ),
        None => None,
    };

    // Validate the optional skill mode at the CLI boundary rather than letting the
    // skill store silently drop a malformed value downstream.
    if let Some(mode) = args.skill_mode.as_deref() {
        let ok = !mode.is_empty() && mode.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
        if !ok {
            return Err(format!(
                "invalid --skill-mode '{mode}' (expected non-empty [A-Za-z0-9-], e.g. `code` or `ask`)"
            ));
        }
    }

    if args.stream_json {
        println!(
            "{}",
            serde_json::json!({
                "type": "session_started",
                "session_id": session_id,
                "resumed": args.session.is_some(),
            })
        );
    } else {
        eprintln!("▶ session {session_id}");
    }
    let data = web::Data::new(state);
    // -p / ExecuteRequest parity (#246 part A): every field `ExecuteRequest`
    // actually carries (model/provider/model_ref, skill_mode, reasoning_effort)
    // is threaded through from CLI flags above and below — none are hardcoded
    // to `None` anymore. `--system-prompt`/`--append-system-prompt`,
    // per-execute `--allowed-tools`/`--disallowed-tools`, and `--max-turns` are
    // intentionally NOT exposed as CLI flags: `ExecuteRequest` has no fields
    // for them today, and adding flags with no server-side effect would be
    // inventing behavior the API doesn't back. `--timeout` IS added, but as a
    // client-side wall-clock cutoff (reuses the Ctrl-C cancellation path
    // below) rather than a request field, for the same reason.
    let response = execute_handler(
        data.clone(),
        web::Path::from(session_id.clone()),
        web::Json(ExecuteRequest {
            model: model_selection.model.clone(),
            provider: model_selection.provider.clone(),
            model_ref: model_selection.model_ref.clone(),
            skill_mode: args.skill_mode.clone(),
            reasoning_effort,
            client_sync: None,
            // #74: a headless `-p` run has no interactive approver. The handler
            // re-derives + persists this onto the root session each execute, so
            // its sub-agents (which inherit it) route gated actions to the
            // off-loop model-reviewer instead of escalating to an absent human.
            no_human_approver: true,
        }),
    )
    .await;
    if !response.status().is_success() {
        return Err(format!("execute rejected: HTTP {}", response.status()));
    }

    // ---- drive the run ----
    // Drain each execution segment to quiescence. When the run pauses on a
    // pending question (e.g. a permission gate, since headless has no UI
    // approver), prompt the operator on the terminal and submit the answer to
    // resume — we do NOT silently bypass. Repeat until the tree finishes with no
    // pending question.
    let mut exit: Result<(), String> = Ok(());
    let mut streamed_tokens = false;
    // `--timeout`: measured from before the FIRST execute, so it covers the
    // whole run wall-clock including any permission-gate round trips across
    // resumed segments below (not reset per segment).
    let overall_start = Instant::now();
    let mut timed_out = false;

    'run: loop {
        let mut saw_terminal = false;
        let mut last_event = Instant::now();
        let mut poll = tokio::time::interval(Duration::from_millis(400));
        let started = Instant::now();

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("\n⏹ cancelling…");
                    if let Some(runner) = data.agent_runners.read().await.get(&session_id) {
                        runner.cancel_token.cancel();
                    }
                }
                ev = events.recv() => {
                    match ev {
                        Ok(event) => {
                            last_event = Instant::now();
                            if let Ok(value) = serde_json::to_value(&event) {
                                if args.stream_json {
                                    println!("{value}");
                                } else {
                                    print_server_event(&value, &mut streamed_tokens);
                                }
                                match value["type"].as_str().unwrap_or("") {
                                    // `need_clarification` also ends this segment: the
                                    // runner is now parked on a pending question.
                                    "complete" | "cancelled" | "need_clarification" => {
                                        saw_terminal = true
                                    }
                                    "error" => {
                                        saw_terminal = true;
                                        exit = Err(value["message"]
                                            .as_str()
                                            .unwrap_or("agent errored")
                                            .to_string());
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = poll.tick() => {
                    // `--timeout`: fire the cancellation once, exactly like Ctrl-C does,
                    // then fall through to the same terminal-event + quiescence draining
                    // below so the "cancelled" event is observed and the tree actually
                    // winds down before we exit.
                    if !timed_out {
                        if let Some(secs) = args.timeout_secs {
                            if overall_start.elapsed() >= Duration::from_secs(secs) {
                                timed_out = true;
                                eprintln!("\n⏰ --timeout {secs}s reached — cancelling…");
                                if let Some(runner) = data.agent_runners.read().await.get(&session_id) {
                                    runner.cancel_token.cancel();
                                }
                            }
                        }
                    }
                    // Startup grace: give the spawn a moment to register the runner.
                    if started.elapsed() < Duration::from_secs(2) {
                        continue;
                    }
                    if saw_terminal
                        && last_event.elapsed() > Duration::from_millis(1200)
                        && tree_quiescent(&data, &session_id).await
                    {
                        break;
                    }
                }
            }
        }

        if timed_out && exit.is_ok() {
            exit = Err(format!(
                "timed out after {}s (cancelled)",
                args.timeout_secs.unwrap_or_default()
            ));
        }

        // Segment quiescent. Stop on error; never prompt in machine (NDJSON) mode.
        // Otherwise resume through a pending question if one is parked, else done.
        if exit.is_err() || args.stream_json {
            break 'run;
        }
        let pending = data
            .storage
            .load_session(&session_id)
            .await
            .ok()
            .flatten()
            .and_then(|session| session.pending_question);
        let Some(pending) = pending else { break 'run };

        let Some(response) = prompt_pending_response(&pending).await else {
            // Either EOF on a TTY or a headless gate with no submittable
            // fail-closed answer (see `HeadlessPromptAction::Abort`). Either way,
            // leave the question unanswered and exit — the gated tool never ran.
            eprintln!("• leaving the question unanswered (headless, fail-closed)");
            break 'run;
        };

        // Re-subscribe before resuming so no event of the resumed segment is lost.
        events = data.get_session_event_sender(&session_id).await.subscribe();
        let resp = submit_response(
            data.clone(),
            web::Path::from(session_id.clone()),
            web::Json(RespondRequest {
                response,
                model: model_selection.model.clone(),
                provider: model_selection.provider.clone(),
                model_ref: model_selection.model_ref.clone(),
                reasoning_effort,
            }),
        )
        .await;
        match resp {
            Ok(http) if http.status().is_success() => {}
            Ok(http) => {
                exit = Err(format!("respond rejected: HTTP {}", http.status()));
                break 'run;
            }
            Err(error) => {
                exit = Err(format!("respond failed: {error}"));
                break 'run;
            }
        }
    }

    // ---- final output ----
    let final_reply = data
        .storage
        .load_session(&session_id)
        .await
        .ok()
        .flatten()
        .and_then(|session| {
            session
                .messages
                .iter()
                .rev()
                .find(|m| matches!(m.role, Role::Assistant))
                .map(|m| m.content.clone())
        });

    if args.stream_json {
        // Terminal envelope: machine-readable summary closing the stream.
        println!(
            "{}",
            serde_json::json!({
                "type": "result",
                "session_id": session_id,
                "status": if exit.is_ok() { "finished" } else { "error" },
                "result": final_reply,
                "error": exit.as_ref().err(),
            })
        );
    } else {
        println!();
        if !streamed_tokens {
            if let Some(reply) = &final_reply {
                println!("{reply}");
            }
        }
        match &exit {
            Ok(()) => eprintln!("✔ finished"),
            Err(e) => eprintln!("✘ {e}"),
        }
        eprintln!("session: {session_id}");
        eprintln!("continue with: bamboo -p \"<next message>\" -s {session_id}");
    }
    exit
}

/// The tree is done when the root runner is not running, the root is not
/// suspended waiting for children, and no child runner is still running.
async fn tree_quiescent(state: &AppState, session_id: &str) -> bool {
    use bamboo_server::app_state::AgentStatus;

    {
        let runners = state.agent_runners.read().await;
        let busy = runners
            .values()
            .any(|r| matches!(r.status, AgentStatus::Running | AgentStatus::Pending));
        if busy {
            return false;
        }
    }
    match state.storage.load_session(session_id).await {
        Ok(Some(session)) => session
            .agent_runtime_state
            .as_ref()
            .map(|s| s.waiting_for_children.is_none())
            .unwrap_or(true),
        _ => true,
    }
}

/// The model/provider selection for a headless run, resolved into the fields
/// the execute handler consumes ([`ExecuteRequest::model`],
/// [`ExecuteRequest::provider`], [`ExecuteRequest::model_ref`]).
///
/// Both `model` and `model_ref` are populated for a chosen model so the request
/// is well-formed for either server cascade (`features.provider_model_ref`
/// on/off) and so `model_ref.provider` drives auxiliary-model resolution. What
/// actually makes the pin authoritative, though, is persisting `model_ref` onto
/// the SESSION before execute (see `run`) — session model is ranked first in
/// both cascades, whereas the request's `model` alone is outranked by the
/// provider's configured default in the legacy (default) cascade.
#[derive(Default, Clone, PartialEq, Eq, Debug)]
struct ModelSelection {
    /// Provider name to pass through when no concrete model was chosen. The
    /// handler resolves that provider's configured default model.
    provider: Option<String>,
    /// Bare model id — the legacy cascade's final fallback.
    model: Option<String>,
    /// A fully-resolved provider+model reference — the new cascade's
    /// highest-priority "request" source.
    model_ref: Option<bamboo_domain::ProviderModelRef>,
}

/// Reconcile `-m`/`--provider` into a [`ModelSelection`].
///
/// One model grammar for the whole `bamboo` binary — the same
/// bare-model-on-default-provider rule `actor run` already uses:
/// - `-m provider:model`         → ref(provider, model)
/// - `--provider P` + `-m model` → ref(P, model)
/// - `-m model` (bare)           → ref(default_provider, model)
/// - `--provider P` (no `-m`)    → provider = P; `run` then resolves P's
///                                 configured default model and pins that ref
/// - neither                     → all defaults flow from the session/config
///
/// A `provider:model` spec whose provider conflicts with `--provider` is an error.
fn resolve_model_selection(
    model: &Option<String>,
    provider: &Option<String>,
    default_provider: &str,
) -> Result<ModelSelection, String> {
    let cli_provider = provider.as_deref().map(str::trim).filter(|p| !p.is_empty());
    let model = model.as_deref().map(str::trim).filter(|m| !m.is_empty());

    // Split an explicit `provider:model` spec, if any.
    let (spec_provider, bare_model) = match model {
        Some(spec) => match spec.split_once(':') {
            Some((p, m)) => {
                let (p, m) = (p.trim(), m.trim());
                if p.is_empty() || m.is_empty() {
                    return Err(format!("-m '{spec}' must be 'provider:model'"));
                }
                (Some(p.to_string()), Some(m.to_string()))
            }
            None => (None, Some(spec.to_string())),
        },
        None => (None, None),
    };

    // Reconcile the provider named inside `-m provider:model` against `--provider`.
    let chosen_provider = match (spec_provider, cli_provider) {
        (Some(a), Some(b)) if !a.eq_ignore_ascii_case(b) => {
            return Err(format!(
                "conflicting providers: -m specifies '{a}', --provider is '{b}' (drop one)"
            ));
        }
        (Some(a), _) => Some(a),
        (None, Some(b)) => Some(b.to_string()),
        (None, None) => None,
    };

    Ok(match (chosen_provider, bare_model) {
        // A model id is present → resolve a concrete provider+model ref (new
        // path) AND carry the bare model as the legacy-path fallback. A bare
        // model with no provider binds to the configured default provider.
        (provider, Some(model)) => {
            let provider = provider.unwrap_or_else(|| default_provider.trim().to_string());
            if provider.is_empty() {
                return Err(format!(
                    "-m '{model}': no provider given and no default provider is configured \
                     (use `provider:model`, add `--provider`, or run `bamboo init`)"
                ));
            }
            ModelSelection {
                provider: None,
                model: Some(model.clone()),
                model_ref: Some(bamboo_domain::ProviderModelRef::new(provider, model)),
            }
        }
        // Provider only → the handler picks that provider's default model.
        (Some(provider), None) => ModelSelection {
            provider: Some(provider),
            model: None,
            model_ref: None,
        },
        // Nothing specified → session/config defaults flow unchanged.
        (None, None) => ModelSelection::default(),
    })
}

/// Pretty-print one server event (typed `AgentEvent`, serialized form).
/// Child (sub-agent) streams are shown indented under a `│` gutter.
fn print_server_event(value: &serde_json::Value, streamed_tokens: &mut bool) {
    use std::io::Write;
    match value["type"].as_str().unwrap_or("") {
        "token" => {
            *streamed_tokens = true;
            print!("{}", value["content"].as_str().unwrap_or(""));
            let _ = std::io::stdout().flush();
        }
        "tool_start" => eprintln!("\n⚙ {}", value["tool_name"].as_str().unwrap_or("tool")),
        "tool_complete" => eprintln!("✔ tool done"),
        "tool_error" => eprintln!("✘ tool error: {}", value["error"].as_str().unwrap_or("")),
        "sub_agent_started" => {
            eprintln!(
                "\n┌ actor {} started",
                value["child_session_id"].as_str().unwrap_or("?")
            );
        }
        "sub_agent_completed" => {
            eprintln!(
                "└ actor {} {}",
                value["child_session_id"].as_str().unwrap_or("?"),
                value["status"].as_str().unwrap_or("done")
            );
        }
        "sub_agent_event" => {
            // Nested child event: surface its tokens/tools with a gutter.
            let inner = &value["event"];
            match inner["type"].as_str().unwrap_or("") {
                "token" => {
                    print!("{}", inner["content"].as_str().unwrap_or(""));
                    let _ = std::io::stdout().flush();
                }
                "tool_start" => {
                    eprintln!("\n│ ⚙ {}", inner["tool_name"].as_str().unwrap_or("tool"))
                }
                "tool_complete" => eprintln!("│ ✔ tool done"),
                _ => {}
            }
        }
        "error" => eprintln!("\n✘ {}", value["message"].as_str().unwrap_or("")),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::PendingQuestion;

    fn permission_gate() -> PendingQuestion {
        PendingQuestion {
            tool_call_id: "call-1".to_string(),
            tool_name: "Bash".to_string(),
            question: "The Bash tool needs approval to run `cargo check`".to_string(),
            options: vec!["Approve".to_string(), "Deny".to_string()],
            allow_custom: false,
            source: Default::default(),
        }
    }

    /// Issue #80: when stdin is NOT a terminal, the headless prompt must resolve
    /// the gate deterministically WITHOUT reading stdin (no hang), and FAIL CLOSED
    /// (deny) rather than auto-approve.
    #[test]
    fn non_tty_fails_closed_to_deny() {
        let pending = permission_gate();
        match resolve_headless_permission(false, &pending) {
            HeadlessPromptAction::Resolve { response, message } => {
                // Resolves to the gate's own `deny` option — never `approve`.
                assert_eq!(response, "Deny");
                assert_ne!(response, "Approve");
                // The message must point the operator at the escape hatch.
                assert!(message.contains("--permission-mode"));
                assert!(message.to_ascii_lowercase().contains("deny"));
            }
            HeadlessPromptAction::Abort { .. } => {
                panic!("a gate with a real Deny option must resolve to it, not abort");
            }
            HeadlessPromptAction::Prompt => {
                panic!("non-TTY must resolve deterministically, not prompt on stdin");
            }
        }
    }

    /// When stdin IS a terminal, keep the existing interactive prompt path.
    #[test]
    fn tty_prompts_interactively() {
        let pending = permission_gate();
        assert!(matches!(
            resolve_headless_permission(true, &pending),
            HeadlessPromptAction::Prompt
        ));
    }

    /// Fail-closed deny works even for a gate with non-standard option labels:
    /// `pick_option` matches the `deny`-containing option case-insensitively, and
    /// falls back to a literal "deny" if none is present (still not "approve").
    #[test]
    fn non_tty_deny_handles_varied_options() {
        let mut pending = permission_gate();
        pending.options = vec!["Allow it".to_string(), "Deny this".to_string()];
        match resolve_headless_permission(false, &pending) {
            HeadlessPromptAction::Resolve { response, .. } => {
                assert_eq!(response, "Deny this");
            }
            HeadlessPromptAction::Abort { .. } => {
                panic!("a gate with a deny-containing option must resolve to it, not abort")
            }
            HeadlessPromptAction::Prompt => panic!("expected deterministic resolve"),
        }
    }

    /// Issue #80 (part a): an Approve-FIRST gate with NO `deny`-labelled option
    /// (e.g. `exit_plan_mode`'s `["Approve …", "Stay in plan mode"]`) must, under a
    /// non-TTY headless run, NEVER resolve to the Approve option — it stays fail
    /// closed regardless of how the no-deny case is handled.
    #[test]
    fn non_tty_approve_first_never_auto_approves() {
        let mut pending = permission_gate();
        pending.options = vec![
            "Approve (proceed with the plan)".to_string(),
            "Stay in plan mode".to_string(),
        ];
        match resolve_headless_permission(false, &pending) {
            HeadlessPromptAction::Resolve { response, .. } => assert!(
                !response.to_ascii_lowercase().contains("approve"),
                "fail-closed must never auto-approve; got {response:?}"
            ),
            // No answer is routed at all → trivially never approves.
            HeadlessPromptAction::Abort { .. } => {}
            HeadlessPromptAction::Prompt => {
                panic!("non-TTY must resolve deterministically, not prompt on stdin")
            }
        }
    }

    /// Issue #80 (part b): the same Approve-FIRST / no-deny-label gate must not
    /// route the guaranteed-invalid literal `"deny"` through `submit_response`
    /// (which would exit on a `respond rejected` error). It resolves to a graceful
    /// `Abort` that points at the escape hatch — still fail-closed, just clean.
    #[test]
    fn non_tty_approve_first_without_deny_label_aborts_gracefully() {
        let mut pending = permission_gate();
        pending.question = "Ready to code? Here is the plan…".to_string();
        pending.options = vec![
            "Approve (proceed with the plan)".to_string(),
            "Keep planning".to_string(),
            "Stay in plan mode".to_string(),
        ];
        pending.allow_custom = false;
        match resolve_headless_permission(false, &pending) {
            HeadlessPromptAction::Abort { message } => {
                assert!(message.to_ascii_lowercase().contains("cannot auto-resolve"));
                assert!(message.contains("--permission-mode"));
            }
            HeadlessPromptAction::Resolve { response, .. } => {
                // Even without part (b) the fallback must never auto-approve, but a
                // guaranteed-invalid literal answer is exactly what (b) removes.
                assert!(
                    !response.to_ascii_lowercase().contains("approve"),
                    "must never auto-approve; got {response:?}"
                );
                panic!(
                    "approve-first gate with no deny option must Abort gracefully, \
                     not route the invalid answer {response:?} through submit_response"
                );
            }
            HeadlessPromptAction::Prompt => panic!("non-TTY must resolve deterministically"),
        }
    }

    /// A gate that accepts custom answers can still submit the literal `"deny"`
    /// even with no deny-labelled option, so it resolves (does not abort).
    #[test]
    fn non_tty_no_deny_label_but_custom_allowed_resolves() {
        let mut pending = permission_gate();
        pending.options = vec!["Approve".to_string(), "Stay in plan mode".to_string()];
        pending.allow_custom = true;
        match resolve_headless_permission(false, &pending) {
            HeadlessPromptAction::Resolve { response, .. } => {
                assert_eq!(response, "deny");
            }
            HeadlessPromptAction::Abort { .. } => {
                panic!("custom-answer gates can submit the literal deny; must not abort")
            }
            HeadlessPromptAction::Prompt => panic!("non-TTY must resolve deterministically"),
        }
    }

    fn some(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    fn model_ref(p: &str, m: &str) -> Option<bamboo_domain::ProviderModelRef> {
        Some(bamboo_domain::ProviderModelRef::new(p, m))
    }

    /// `-m provider:model` resolves to that exact ref, provider inferred from it.
    /// Both the ref (new cascade) and the bare model (legacy cascade) are set.
    #[test]
    fn model_selection_colon_form() {
        let sel = resolve_model_selection(&some("openai:gpt-4o"), &None, "anthropic").unwrap();
        assert_eq!(sel.model_ref, model_ref("openai", "gpt-4o"));
        assert_eq!(sel.model, some("gpt-4o"));
        assert_eq!(sel.provider, None);
    }

    /// A bare `-m <model>` binds to the configured DEFAULT provider (mirrors
    /// `actor run`) so it reliably wins over the provider default.
    #[test]
    fn model_selection_bare_model_uses_default_provider() {
        let sel = resolve_model_selection(&some("gpt-4o"), &None, "openai").unwrap();
        assert_eq!(sel.model_ref, model_ref("openai", "gpt-4o"));
        assert_eq!(sel.model, some("gpt-4o"));
        assert_eq!(sel.provider, None);
    }

    /// `--provider P` + a bare `-m <model>` compose into `ref(P, model)`.
    #[test]
    fn model_selection_provider_flag_plus_bare_model() {
        let sel = resolve_model_selection(&some("gpt-4o"), &some("openai"), "anthropic").unwrap();
        assert_eq!(sel.model_ref, model_ref("openai", "gpt-4o"));
    }

    /// `--provider P` alone passes the provider through (no ref); the handler
    /// picks that provider's default model.
    #[test]
    fn model_selection_provider_only() {
        let sel = resolve_model_selection(&None, &some("gemini"), "anthropic").unwrap();
        assert_eq!(sel.model_ref, None);
        assert_eq!(sel.provider, some("gemini"));
    }

    /// Nothing specified → all defaults flow from the session/config downstream.
    #[test]
    fn model_selection_empty_is_default() {
        let sel = resolve_model_selection(&None, &None, "anthropic").unwrap();
        assert_eq!(sel, ModelSelection::default());
    }

    /// `-m provider:model` colliding with a different `--provider` is rejected.
    #[test]
    fn model_selection_conflicting_providers_error() {
        let err = resolve_model_selection(&some("openai:gpt-4o"), &some("anthropic"), "anthropic")
            .unwrap_err();
        assert!(err.contains("conflicting providers"), "got: {err}");
    }

    /// A matching (case-insensitive) `--provider` alongside `provider:model` is fine.
    #[test]
    fn model_selection_matching_provider_ok() {
        let sel = resolve_model_selection(&some("OpenAI:gpt-4o"), &some("openai"), "x").unwrap();
        assert_eq!(sel.model_ref, model_ref("OpenAI", "gpt-4o"));
    }

    /// Malformed colon specs are rejected with a clear message.
    #[test]
    fn model_selection_malformed_colon_error() {
        assert!(resolve_model_selection(&some("openai:"), &None, "x").is_err());
        assert!(resolve_model_selection(&some(":gpt-4o"), &None, "x").is_err());
    }

    /// A bare model with neither `--provider` nor a configured default errors
    /// actionably rather than silently constructing an empty-provider ref.
    #[test]
    fn model_selection_bare_model_no_default_errors() {
        let err = resolve_model_selection(&some("gpt-4o"), &None, "   ").unwrap_err();
        assert!(err.contains("no provider"), "got: {err}");
    }

    /// `--timeout` omitted never times out.
    #[test]
    fn timeout_absent_is_valid() {
        assert!(validate_timeout_secs(None).is_ok());
    }

    /// Any positive number of seconds is accepted.
    #[test]
    fn timeout_positive_is_valid() {
        assert!(validate_timeout_secs(Some(1)).is_ok());
        assert!(validate_timeout_secs(Some(3600)).is_ok());
    }

    /// `--timeout 0` is rejected — it would fire immediately, which is never
    /// the intent and more likely a typo.
    #[test]
    fn timeout_zero_is_rejected() {
        let err = validate_timeout_secs(Some(0)).unwrap_err();
        assert!(err.contains("--timeout"), "got: {err}");
    }
}
