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

/// Prompt the operator on the terminal for a parked pending question (typically a
/// permission gate, since headless has no UI approver) and map the answer to a
/// response string the engine's respond flow understands: `y`/`yes` → `approve`,
/// `n`/`no`/empty → `deny`, a number selects a listed option, anything else is
/// sent verbatim. Returns `None` on EOF (e.g. stdin is not a TTY).
async fn prompt_pending_response(pending: &bamboo_agent_core::PendingQuestion) -> Option<String> {
    use std::io::Write as _;
    use tokio::io::{AsyncBufReadExt, BufReader};

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
}

pub async fn run(args: HeadlessArgs) -> Result<(), String> {
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
    {
        let mut session = state
            .storage
            .load_session(&session_id)
            .await
            .map_err(|e| format!("load session: {e}"))?
            .ok_or_else(|| "session vanished".to_string())?;
        session.add_message(Message::user(args.prompt.clone()));
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

    let model_ref = parse_model_ref(&args.model)?;

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
    let response = execute_handler(
        data.clone(),
        web::Path::from(session_id.clone()),
        web::Json(ExecuteRequest {
            model: None,
            provider: None,
            model_ref,
            skill_mode: None,
            reasoning_effort: None,
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
            eprintln!("• no input (EOF) — leaving the question unanswered");
            break 'run;
        };

        // Re-subscribe before resuming so no event of the resumed segment is lost.
        events = data.get_session_event_sender(&session_id).await.subscribe();
        let resp = submit_response(
            data.clone(),
            web::Path::from(session_id.clone()),
            web::Json(RespondRequest {
                response,
                model: None,
                provider: None,
                model_ref: None,
                reasoning_effort: None,
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

fn parse_model_ref(
    model: &Option<String>,
) -> Result<Option<bamboo_domain::ProviderModelRef>, String> {
    let Some(spec) = model else { return Ok(None) };
    let spec = spec.trim();
    let Some((p, m)) = spec.split_once(':') else {
        return Err(format!(
            "-m '{spec}' must be 'provider:model' in server mode (see config defaults otherwise)"
        ));
    };
    if p.trim().is_empty() || m.trim().is_empty() {
        return Err(format!("-m '{spec}' must be provider:model"));
    }
    Ok(Some(bamboo_domain::ProviderModelRef::new(
        p.trim(),
        m.trim(),
    )))
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
