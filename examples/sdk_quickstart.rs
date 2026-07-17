//! In-process Rust SDK quick-start — the same agent loop the server runs, with
//! no HTTP and no server process.
//!
//! This is the compile-checked twin of the "Use it as a Rust SDK (in-process)"
//! section in the crate README: if the README snippet drifts from the real API,
//! this example stops compiling. Run it against a configured data dir:
//!
//! ```bash
//! cargo run --example sdk_quickstart
//! ```
//!
//! It requires `~/.bamboo/config.json` to define a provider + API key (the same
//! config `bamboo serve` reads); `with_defaults_for_data_dir` wires the runtime
//! dependencies from that directory.
//!
//! This example also demonstrates the two interactive capabilities layered on
//! top of "start a run and stream events" (bamboo-agent#244):
//!
//! - **Cancellation**: `run_stream_cancellable` hands back a `CancellationToken`
//!   alongside the event receiver; a background watchdog cancels the run if it
//!   runs unexpectedly long.
//! - **Approval / clarification + resume**: if the model calls a tool that
//!   pauses for user input (`conclusion_with_options`, or a gated tool under a
//!   configured `PermissionChecker`), the event loop sees
//!   `AgentEvent::NeedClarification` / `ToolApprovalRequested`, answers it via
//!   `Agent::answer`, and resumes the run via `Agent::resume_stream` — the
//!   "ask → answer → resume" flow. When the approved question was a gated tool
//!   call, resuming also RE-EXECUTES that tool for real (against the agent's
//!   own tool executor) and writes the genuine output back before the loop
//!   continues — no extra code needed, it happens automatically inside
//!   `resume`/`resume_stream` (bamboo-agent#509).
//!
//!   A separate mechanism, `AgentEvent::ChildApprovalRequested`, covers an
//!   out-of-process CHILD sub-agent's gated tool (only relevant if you've also
//!   wired the engine's actor/broker transport — `with_defaults_for_data_dir`
//!   does not). Answer those with `Agent::answer_child_approval`, not
//!   `Agent::answer`.

use std::time::Duration;

use bamboo_sdk::agent::{Agent, AgentEvent, Session};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");

    // Build the agent. One call assembles storage, persistence, skills,
    // metrics, the provider (from ~/.bamboo/config.json), and the default
    // built-in tool set — no manual dependency wiring.
    let agent = Agent::builder()
        .model("claude-sonnet-4-6")
        .instruction("You are a helpful coding agent.")
        .with_defaults_for_data_dir(home)
        .await
        .expect("wire runtime deps")
        .build()
        .expect("agent fully configured");

    let session_id = "demo-session".to_string();
    let session = Session::new(session_id.clone(), "claude-sonnet-4-6");

    // Stream one turn: `run_stream_cancellable` appends the user message, runs
    // the loop on a background task, and hands back a receiver of AgentEvents
    // PLUS a `CancellationToken` — call `.cancel()` from any other task to
    // interrupt the run at its next check point.
    let (mut rx, cancel_token) = agent.run_stream_cancellable(
        session,
        "List the files here and tell me what this project does.",
    );

    // Cancellation demo: a watchdog that stops a run which takes too long.
    // (In a real app this would be wired to a user-facing "Stop" button
    // instead of a fixed timeout.)
    let watchdog_token = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(120)).await;
        watchdog_token.cancel();
    });

    while let Some(event) = rx.recv().await {
        // Approval / clarification + resume demo: the loop suspended waiting
        // for input (a `conclusion_with_options` call, or a gated tool under a
        // configured permission checker). Both suspend via the same
        // `session.pending_question` mechanism, so both are answered the same
        // way. Answer it, then resume the run from where it left off — if this
        // was a permission approval, `answer_and_resume_stream` also
        // re-executes the gated tool for real before the loop continues, so
        // the model sees genuine output rather than an inferred placeholder.
        let needs_answer = matches!(
            event,
            AgentEvent::NeedClarification { .. } | AgentEvent::ToolApprovalRequested { .. }
        );
        println!("{event:?}");

        if needs_answer {
            println!("agent is asking for input — auto-approving");
            let mut resumed_rx = agent
                .answer_and_resume_stream(session_id.clone(), "Approve")
                .await
                .expect("answer + resume");
            while let Some(event) = resumed_rx.recv().await {
                println!("{event:?}");
            }
            break;
        }
    }

    Ok(())
}
