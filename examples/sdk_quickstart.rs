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

use bamboo_sdk::agent::{Agent, Session};

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

    // Stream one turn: `run_stream` appends the user message, runs the loop on
    // a background task, and hands back a receiver of AgentEvents.
    let session = Session::new("demo-session", "claude-sonnet-4-6");
    let mut rx = agent.run_stream(
        session,
        "List the files here and tell me what this project does.",
    );
    while let Some(event) = rx.recv().await {
        // assistant text, tool calls, tool results, token usage, completion
        println!("{event:?}");
    }

    Ok(())
}
