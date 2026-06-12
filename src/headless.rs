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

pub struct HeadlessArgs {
    pub prompt: String,
    /// Continue this existing root session instead of creating a new one.
    pub session: Option<String>,
    pub model: Option<String>,
    pub workspace: Option<PathBuf>,
    pub data_dir: PathBuf,
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
        }),
    )
    .await;
    if !response.status().is_success() {
        return Err(format!("execute rejected: HTTP {}", response.status()));
    }

    // ---- drain until the whole tree is quiescent ----
    let mut exit: Result<(), String> = Ok(());
    let mut streamed_tokens = false;
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
                                "complete" | "cancelled" => saw_terminal = true,
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
