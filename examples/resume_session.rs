//! Continuing an existing [`Session`] across turns — the ordinary multi-turn
//! chat pattern (distinct from `sdk_quickstart.rs`'s ask/answer/resume, which
//! resumes a run suspended mid-turn on a clarification/approval).
//!
//! Every `Agent::run`/`run_stream` call persists the session as it executes
//! (via the storage/persistence deps `with_defaults_for_data_dir` wires up),
//! so a session created in one process can be reloaded and continued in a
//! later call — or even a later process — by id by via
//! [`Agent::get_session`]/[`Agent::list_sessions`].
//!
//! ```bash
//! cargo run --example resume_session
//! ```
//!
//! Needs `~/.bamboo/config.json` with a provider + API key (`bamboo init`).

use bamboo_sdk::agent::{Agent, Session};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");

    let agent = Agent::builder()
        .model("claude-sonnet-4-6")
        .instruction("You are a helpful coding agent.")
        .with_defaults_for_data_dir(home)
        .await
        .expect("wire runtime deps")
        .build()
        .expect("agent fully configured");

    let session_id = "resume-session-demo".to_string();

    // --- Turn 1: reuse a persisted session if one exists, else start fresh.
    //
    // `get_session` returns `Ok(None)` for an id that hasn't been used yet —
    // that's the normal "first turn" case, not an error.
    let mut session = agent
        .get_session(&session_id)
        .await
        .expect("session lookup")
        .unwrap_or_else(|| Session::new(session_id.clone(), "claude-sonnet-4-6"));

    agent
        .run(
            &mut session,
            "My favorite programming language is Rust. Remember that.",
        )
        .await
        .expect("first turn");
    println!(
        "[turn 1] {}",
        session
            .messages
            .last()
            .map(|m| m.content.as_str())
            .unwrap_or("")
    );

    // --- Turn 2: reload the session by id — as a fresh process restarting
    // this example would — and continue the SAME conversation. `run` appends
    // the new user message onto the existing history and picks up right
    // where the last turn left off; the model still has turn 1's context.
    let mut session = agent
        .get_session(&session_id)
        .await
        .expect("session lookup")
        .expect("session was just saved by turn 1");

    agent
        .run(&mut session, "What's my favorite programming language?")
        .await
        .expect("second turn");
    println!(
        "[turn 2] {}",
        session
            .messages
            .last()
            .map(|m| m.content.as_str())
            .unwrap_or("")
    );

    // Every session lives in `list_sessions()`, most-recently-updated first —
    // handy for a "recent conversations" list in an embedding app.
    let sessions = agent.list_sessions().await.expect("list_sessions");
    println!(
        "\n{} session(s) on disk, most recent first:",
        sessions.len()
    );
    for entry in sessions.iter().take(5) {
        println!("  {} (updated_at={:?})", entry.id, entry.updated_at);
    }

    Ok(())
}
