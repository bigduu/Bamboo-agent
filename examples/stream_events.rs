//! Streaming a run and matching real `AgentEvent` variants — the shape a
//! real embedder needs (not `println!("{event:?}")`).
//!
//! `sdk_quickstart.rs` demonstrates the overall builder + ask/answer/resume +
//! cancellation flow but only Debug-prints events. This example shows how to
//! actually *consume* the event stream: render assistant text incrementally,
//! surface tool calls/results, track token usage, and detect the terminal
//! events (`Complete` / `Cancelled` / `Error`).
//!
//! ```bash
//! cargo run --example stream_events
//! ```
//!
//! Like `sdk_quickstart.rs`, this needs `~/.bamboo/config.json` to define a
//! provider + API key (`bamboo init`) — `with_defaults_for_data_dir` reads it.

use bamboo_sdk::agent::{Agent, AgentEvent, Session};

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

    let session = Session::new("stream-events-demo", "claude-sonnet-4-6");
    let mut rx = agent.run_stream(session, "List the files here and summarize this project.");

    // Track assistant text as it streams in, so we can print it as one line
    // at the end instead of interleaving raw tokens with tool-call output.
    let mut assistant_text = String::new();

    while let Some(event) = rx.recv().await {
        match event {
            // -- Text generation -------------------------------------------
            AgentEvent::Token { content } => {
                // Streamed assistant text — append and optionally render
                // incrementally (e.g. push to a UI text buffer).
                assistant_text.push_str(&content);
            }
            AgentEvent::ReasoningToken { content } => {
                // Model "thinking" trace, streamed on a separate channel so a
                // UI can choose whether to show it at all.
                print!("{content}");
            }

            // -- Tool execution ----------------------------------------------
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                println!("\n[tool start] {tool_name} ({tool_call_id}) args={arguments}");
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                println!(
                    "[tool complete] {tool_call_id} success={} result={}",
                    result.success,
                    // Tool output can be large; truncate for display.
                    result.result.chars().take(200).collect::<String>()
                );
            }
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => {
                println!("[tool error] {tool_call_id}: {error}");
            }

            // -- User interaction --------------------------------------------
            AgentEvent::NeedClarification {
                question, options, ..
            } => {
                // The run has suspended waiting for input — see
                // `sdk_quickstart.rs` / `resume_session.rs` for the
                // `agent.answer(...)` + `agent.resume_stream(...)` flow that
                // continues past this point.
                println!("\n[needs clarification] {question} options={options:?}");
            }
            AgentEvent::ToolApprovalRequested {
                tool_call_id,
                tool_name,
                parameters,
            } => {
                println!("\n[approval requested] {tool_name} ({tool_call_id}) params={parameters}");
            }

            // -- Progress tracking --------------------------------------------
            AgentEvent::TaskListUpdated { task_list, .. } => {
                println!("\n[task list updated] {} item(s)", task_list.items.len());
            }

            // -- Context management -------------------------------------------
            AgentEvent::TokenBudgetUpdated { .. } => {
                // Context-window budget changed (compression, summary, etc.).
                // Omitted from display here — see the `AgentEvent` docs for
                // the full payload shape.
            }

            // -- Terminal events ------------------------------------------------
            AgentEvent::Complete { usage } => {
                println!("\n\n--- assistant ---\n{assistant_text}");
                println!(
                    "\n[complete] prompt={} completion={} total={}",
                    usage.prompt_tokens, usage.completion_tokens, usage.total_tokens
                );
                break;
            }
            AgentEvent::Cancelled { message } => {
                println!("\n[cancelled] {message:?}");
                break;
            }
            AgentEvent::Error { message } => {
                println!("\n[error] {message}");
                break;
            }

            // Every other variant (sub-agent events, session lifecycle,
            // notifications, …) is safe to ignore in a minimal consumer —
            // the wildcard keeps this example resilient to new variants
            // being added to the enum.
            _other => {}
        }
    }

    Ok(())
}
